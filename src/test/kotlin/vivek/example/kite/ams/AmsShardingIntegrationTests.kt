package vivek.example.kite.ams

import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlin.system.measureTimeMillis
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.await
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Import
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import vivek.example.kite.ams.config.AmsProperties
import vivek.example.kite.ams.shard.AmsTestConfig
import vivek.example.kite.ams.shard.ShardingStrategy
import vivek.example.kite.common.config.CommonProperties
import vivek.example.kite.common.service.SymbolService
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(locations = ["classpath:application-test.yml"])
@ExtendWith(OutputCaptureExtension::class)
@Import(AmsTestConfig::class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class AmsShardingIntegrationTests {

  @Autowired private lateinit var clock: Clock

  @Autowired private lateinit var jmsTopicTemplate: JmsTemplate

  @Autowired private lateinit var commonProperties: CommonProperties

  @Autowired private lateinit var amsProperties: AmsProperties

  @Autowired private lateinit var shardingStrategy: ShardingStrategy

  @Autowired private lateinit var symbolService: SymbolService

  @Test
  fun `should route messages to the correct dynamically assigned shard`(output: CapturedOutput) {
    // --- Setup ---
    // Get symbols and shards defined ONLY in the "test" profile.
    // Due to the fix in application-test.yml, this will be an isolated list.
    val allSymbols = symbolService.getAllSymbols() // Will be [RELIANCE, INFY, WIPRO]
    val availableShards = amsProperties.shards.keys.toList() // Will be [test-shard-1, test-shard-2]
    val assignments = shardingStrategy.assign(allSymbols, availableShards)

    // Dynamically find which shard is responsible for our test symbols
    val relianceShard = assignments.entries.find { "RELIANCE" in it.value }?.key
    val wiproShard = assignments.entries.find { "WIPRO" in it.value }?.key

    assertNotNull(relianceShard, "RELIANCE should be assigned to a test shard")
    assertNotNull(wiproShard, "WIPRO should be assigned to a test shard")

    // From MockAlertRepository, we know:
    // RELIANCE has a GTE alert at 2800.0 * 1.01 = 2828.00
    // WIPRO has a GTE alert at 450.0 * 1.01 = 454.50

    val relianceWindow =
        AggregatedLHWindow(
            symbol = "RELIANCE",
            windowStartTime = Instant.now(clock).toEpochMilli() - 500,
            windowEndTime = Instant.now(clock).toEpochMilli(),
            low = BigDecimal("2825.00"),
            high = BigDecimal("2830.00"), // This high (2830) is >= the alert threshold (2828)
            ticksCount = 10)

    val wiproWindow =
        AggregatedLHWindow(
            symbol = "WIPRO",
            windowStartTime = Instant.now(clock).toEpochMilli() - 500,
            windowEndTime = Instant.now(clock).toEpochMilli(),
            low = BigDecimal("452.00"),
            high = BigDecimal("455.00"), // This high (455) is >= the alert threshold (454.50)
            ticksCount = 10)

    // --- Action ---
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, relianceWindow) {
        message ->
      message.setStringProperty("stockSymbol", "RELIANCE")
      message
    }
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, wiproWindow) { message
      ->
      message.setStringProperty("stockSymbol", "WIPRO")
      message
    }

    // --- Assertion ---
    await.withPollInterval(Duration.ofMillis(2000)).atMost(Duration.ofSeconds(10)).until {
      val log = output.all
      println("Polling log $log")
      log.contains("[$relianceShard] \u001B[33mMatched Alerts for\u001B[0m RELIANCE:") &&
          log.contains("[$wiproShard] \u001B[33mMatched Alerts for\u001B[0m WIPRO:") &&
          log.contains("<- Finished processing for 'RELIANCE'") &&
          log.contains("<- Finished processing for 'WIPRO'")
    }

    val logOutput = output.all

    // Verify correct shard processed each message
    assertTrue(
        logOutput.contains("[$relianceShard] \u001B[33mMatched Alerts for\u001B[0m RELIANCE") &&
            logOutput.contains("RELIANCE_gte_2828.00"),
        "Shard '$relianceShard' should have processed RELIANCE and triggered a GTE alert")
    assertTrue(
        logOutput.contains("[$wiproShard] \u001B[33mMatched Alerts for\u001B[0m WIPRO") &&
            logOutput.contains("WIPRO_gte_454.50"),
        "Shard '$wiproShard' should have processed WIPRO and triggered a GTE alert")

    // Verify cross-contamination did not happen
    if (availableShards.size > 1 && relianceShard != wiproShard) {
      assertTrue(
          !logOutput.contains("[$wiproShard] \u001B[33mMatched Alerts for\u001B[0m RELIANCE"),
          "Shard '$wiproShard' should NOT have processed the RELIANCE message.")
      assertTrue(
          !logOutput.contains("[$relianceShard] \u001B[33mMatched Alerts for\u001B[0m WIPRO"),
          "Shard '$relianceShard' should NOT have processed the WIPRO message.")
    }
  }

  @Test
  fun `should process messages for the same shard concurrently`(output: CapturedOutput) {
    // --- Setup ---
    val allSymbols = symbolService.getAllSymbols()
    val availableShards = amsProperties.shards.keys.toList()
    val assignments = shardingStrategy.assign(allSymbols, availableShards)

    val targetEntry = assignments.entries.find { it.value.size >= 2 }!!
    assertNotNull(
        targetEntry,
        "Test requires at least one shard to have 2+ symbols assigned. Check test config.")

    val symbol1 = targetEntry.value[0]
    val symbol2 = targetEntry.value[1]

    val window1 =
        AggregatedLHWindow(
            symbol = symbol1,
            windowStartTime = 1,
            windowEndTime = 2,
            low = BigDecimal.ZERO,
            high = BigDecimal.ONE,
            ticksCount = 1)
    val window2 =
        AggregatedLHWindow(
            symbol = symbol2,
            windowStartTime = 1,
            windowEndTime = 2,
            low = BigDecimal.ZERO,
            high = BigDecimal.ONE,
            ticksCount = 1)

    // --- Action ---
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window1) { m ->
      m.apply { setStringProperty("stockSymbol", symbol1) }
    }
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window2) { m ->
      m.apply { setStringProperty("stockSymbol", symbol2) }
    }

    // Wait until both messages are fully processed.
    await.atMost(Duration.ofSeconds(5)).until {
      output.all.contains("<- Finished processing for '$symbol1'") &&
          output.all.contains("<- Finished processing for '$symbol2'")
    }

    // --- Assertion ---
    // Verify from logs that they ran on different threads
    val log = output.all
    val thread1 =
        "-> Processing message for '$symbol1' on thread '([^']*)'"
            .toRegex()
            .find(log)
            ?.groupValues
            ?.get(1)
    val thread2 =
        "-> Processing message for '$symbol2' on thread '([^']*)'"
            .toRegex()
            .find(log)
            ?.groupValues
            ?.get(1)

    assertNotNull(thread1, "Could not find thread name for $symbol1 in logs")
    assertNotNull(thread2, "Could not find thread name for $symbol2 in logs")

    // The core assertion: prove that the thread names were different.
    assertNotEquals(
        thread1,
        thread2,
        "Messages for different symbols ('$symbol1' and '$symbol2') in the same shard should be processed by different threads. Found: T1=$thread1, T2=$thread2")
  }

  @Test
  fun `should process messages concurrently up to the concurrency limit`(output: CapturedOutput) {
    // --- Setup ---
    // With our PredictableShardingStrategy, we know the first shard ('test-shard-1')
    // will get the first 4 symbols. The concurrency for this shard is '1-3'.
    val targetShard = "test-shard-1"
    val symbolsToTest = symbolService.getAllSymbols().take(4)
    println("Testing concurrency on predictable shard '$targetShard' with symbols: $symbolsToTest")

    val windows =
        symbolsToTest.map { symbol ->
          AggregatedLHWindow(
              symbol = symbol,
              windowStartTime = 1,
              windowEndTime = 2,
              low = BigDecimal.ZERO,
              high = BigDecimal.ONE,
              ticksCount = 1)
        }

    // --- Action ---
    val processingTime = measureTimeMillis {
      // *** THE FIX IS HERE ***
      // Use runBlocking with launch to send all messages concurrently from the test.
      // This ensures they arrive at the broker at roughly the same time,
      // forcing the broker to use multiple listener threads for load balancing.
      runBlocking(Dispatchers.IO) {
        windows.forEach { window ->
          launch {
            jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window) { m ->
              m.apply { setStringProperty("stockSymbol", window.symbol) }
            }
          }
        }
      }

      // Wait until all four messages are fully processed.
      await.atMost(Duration.ofSeconds(10)).until {
        val log = output.all
        symbolsToTest.all { symbol -> log.contains("<- Finished processing for '$symbol'") }
      }
    }

    // --- Assertion ---
    println(
        "Total processing time for four 1000ms jobs with concurrency '1-3': ${processingTime}ms")

    // With a concurrency of 3, the first 3 jobs run in parallel (~1000ms).
    // The 4th job starts after the first one finishes, so total time should be ~2000ms.
    // We assert it's much less than the sequential time of 4000ms and more than one parallel batch
    // (1000ms).
    assertTrue(
        processingTime in 1900..2800,
        "Total time should be approx 2 seconds, not 4. Actual: ${processingTime}ms")

    // Verify from logs that exactly 3 unique threads were used for the 4 tasks.
    val log = output.all
    val threadNames =
        symbolsToTest
            .mapNotNull { symbol ->
              "-> Processing message for '$symbol' on thread '([^']*)'"
                  .toRegex()
                  .find(log)
                  ?.groupValues
                  ?.get(1)
            }
            .toSet()

    assertEquals(
        3,
        threadNames.size,
        "Expected exactly 3 unique threads for 4 tasks on a '1-3' concurrency listener. Found threads: $threadNames")
  }
}
