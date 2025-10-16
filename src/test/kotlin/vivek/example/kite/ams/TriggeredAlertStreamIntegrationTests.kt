package vivek.example.kite.ams

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.fail
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.jms.core.JmsTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.shard.ShardManager
import vivek.example.kite.common.config.CommonProperties
import vivek.example.kite.common.service.SymbolService
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles("test")
@TestPropertySource(locations = ["classpath:application-test.yml"])
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class TriggeredAlertStreamIntegrationTests {

  @Autowired private lateinit var jmsTopicTemplate: JmsTemplate

  @Autowired private lateinit var commonProperties: CommonProperties

  @Autowired private lateinit var objectMapper: ObjectMapper
  @Autowired private lateinit var shardManager: ShardManager
  @Autowired private lateinit var symbolService: SymbolService

  @BeforeEach
  fun waitForDataInitialization() {
    // DataInitializer runs on startup. We must wait for all mock alerts to be created
    // and propagated through the outbox pattern to the L1 cache before running any tests.
    val totalMockAlerts = symbolService.getAllSymbols().size * 5 // Mock repo creates 5 per symbol
    await.atMost(Duration.ofSeconds(15)).until {
      val allCachedAlerts =
          symbolService
              .getAllSymbols()
              .flatMap { symbol ->
                shardManager.getShardForSymbol(symbol)?.cache?.getActiveAlerts() ?: emptySet()
              }
              .toSet()
      allCachedAlerts.size >= totalMockAlerts
    }
  }

  @Test
  fun `should publish triggered alert to JMS and stream it via SSE controller`() {
    val testSymbol = "RELIANCE"
    val testUserId = "user_common"

    val testShard = shardManager.getShardForSymbol(testSymbol)!!
    assertNotNull(testShard)
    val alertKey = "${testSymbol}_gte_2828.00"
    val userId = "user_common"
    val targetAlert =
        testShard.cache.getActiveAlerts().firstOrNull {
          it.alertId == alertKey && it.userId == userId
        }!!
    assertNotNull(targetAlert)
    assertEquals(alertKey, targetAlert.alertId)
    assertEquals(testUserId, targetAlert.userId)

    val window =
        AggregatedLHWindow(
            symbol = testSymbol,
            windowStartTime = 1L,
            windowEndTime = 2L,
            low = BigDecimal("2825.00"),
            high = BigDecimal("2830.00"),
            ticksCount = 5)

    val client = WebTestClient.bindToServer().baseUrl("http://localhost:9090").build()

    val testFuture =
        CompletableFuture.supplyAsync {
          client
              .get()
              .uri("/api/v1/alerts/stream?userId=user_common&stockSymbol=RELIANCE")
              .accept(MediaType.TEXT_EVENT_STREAM)
              .exchange()
              .expectStatus()
              .isOk
              .returnResult(String::class.java)
              .responseBody
              .take(1)
              .timeout(Duration.ofSeconds(10))
              .blockFirst()
        }

    // publish JMS message after subscription
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window) { message ->
      message.setStringProperty("stockSymbol", testSymbol)
      message
    }

    val eventString =
        testFuture.get(10, TimeUnit.SECONDS) ?: error("No SSE event received within timeout")
    assertNotNull(eventString, "Did not receive SSE event")

    val json = eventString.substringAfter("data:").trim()
    val typeRef = object : TypeReference<TriggeredAlertEvent>() {}
    val receivedEvent = objectMapper.readValue(json, typeRef)

    assertEquals(targetAlert.alertId, receivedEvent.alert.alertId)
    assertEquals(testUserId, receivedEvent.alert.userId)
    assertEquals(testSymbol, receivedEvent.alert.stockSymbol)
    assertEquals(0, window.high.compareTo(receivedEvent.triggeredPrice))
  }

  @Test
  fun `should handle multiple alert scenarios`() {
    val testShard = shardManager.getShardForSymbol("RELIANCE")
    assertNotNull(testShard, "Test shard for RELIANCE not found")

    // Get the symbol's alerts container
    val symbolAlerts = testShard!!.cache.getAlertsForSymbol("RELIANCE")!!
    assertNotNull(symbolAlerts, "No alerts found for RELIANCE in the cache")

    // Helper function to get the first alert from a condition map
    fun getFirstAlert(conditionType: ConditionType): Alert? {
      val alertsList =
          when (conditionType) {
            ConditionType.GTE,
            ConditionType.GT -> symbolAlerts.upperBoundAlerts
            ConditionType.LTE,
            ConditionType.LT -> symbolAlerts.lowerBoundAlerts
            ConditionType.EQ -> symbolAlerts.equalsAlerts
          }

      return alertsList.entries
          .firstOrNull { entry -> entry.value.any { it.conditionType == conditionType } }
          ?.let { entry ->
            entry.value
                .firstOrNull { it.conditionType == conditionType }
                ?.let { alertDetail -> testShard.cache.getAlertDetails(alertDetail.id) }
          }
    }

    // Get alerts by condition type
    val gteAlert = getFirstAlert(ConditionType.GTE)
    val lteAlert = getFirstAlert(ConditionType.LTE)
    val gtAlert = getFirstAlert(ConditionType.GT)
    val ltAlert = getFirstAlert(ConditionType.LT)

    // Test case 1: Price crosses above the GTE alert price
    gteAlert?.let { alert ->
      testAlertTrigger(
          symbol = alert.stockSymbol,
          userId = alert.userId,
          alertKey = alert.alertId,
          window =
              AggregatedLHWindow(
                  symbol = alert.stockSymbol,
                  windowStartTime = 1L,
                  windowEndTime = 2L,
                  low = alert.priceThreshold - BigDecimal("3.00"),
                  high = alert.priceThreshold + BigDecimal("2.00"),
                  ticksCount = 5),
          expectedTriggeredPrice = { it.high },
          priceComparison = { expected, actual ->
            assertEquals(
                0, expected.compareTo(actual), "Price should be exactly $expected but was $actual")
          })
    } ?: fail("GTE alert not found in test data")

    // Test case 2: Price crosses below the LTE alert price
    lteAlert?.let { alert ->
      testAlertTrigger(
          symbol = alert.stockSymbol,
          userId = alert.userId,
          alertKey = alert.alertId,
          window =
              AggregatedLHWindow(
                  symbol = alert.stockSymbol,
                  windowStartTime = 3L,
                  windowEndTime = 4L,
                  low = alert.priceThreshold - BigDecimal("3.00"),
                  high = alert.priceThreshold + BigDecimal("1.00"),
                  ticksCount = 5),
          expectedTriggeredPrice = { it.low },
          priceComparison = { expected, actual ->
            assertTrue(
                actual <= alert.priceThreshold,
                "Price should be <= ${alert.priceThreshold} but was $actual")
          })
    } ?: fail("LTE alert not found in test data")

    // Test case 3: Price above GT alert price
    gtAlert?.let { alert ->
      testAlertTrigger(
          symbol = alert.stockSymbol,
          userId = alert.userId,
          alertKey = alert.alertId,
          window =
              AggregatedLHWindow(
                  symbol = alert.stockSymbol,
                  windowStartTime = 5L,
                  windowEndTime = 6L,
                  low = alert.priceThreshold - BigDecimal("2.00"),
                  high = alert.priceThreshold + BigDecimal("0.01"),
                  ticksCount = 5),
          expectedTriggeredPrice = { it.high },
          priceComparison = { expected, actual ->
            assertTrue(
                actual > alert.priceThreshold,
                "Price should be > ${alert.priceThreshold} but was $actual")
          })
    } ?: fail("GT alert not found in test data")

    // Test case 4: Price within alert range but not triggering
    ltAlert?.let { alert ->
      testNoAlertTriggered(
          symbol = alert.stockSymbol,
          userId = alert.userId,
          id = alert.id,
          alertKey = alert.alertId,
          window =
              AggregatedLHWindow(
                  symbol = alert.stockSymbol,
                  windowStartTime = 7L,
                  windowEndTime = 8L,
                  low = alert.priceThreshold + BigDecimal("1.00"),
                  high = alert.priceThreshold + BigDecimal("2.00"),
                  ticksCount = 5))
    } ?: fail("LT alert not found in test data")
  }

  private fun testAlertTrigger(
      symbol: String,
      userId: String,
      alertKey: String,
      window: AggregatedLHWindow,
      expectedTriggeredPrice: (AggregatedLHWindow) -> BigDecimal,
      priceComparison: (BigDecimal, BigDecimal) -> Unit
  ) {
    val testShard = shardManager.getShardForSymbol(symbol)!!
    assertNotNull(testShard, "Shard for symbol $symbol should not be null")

    val targetAlert =
        testShard.cache.getActiveAlerts().firstOrNull {
          it.alertId == alertKey && it.userId == userId
        }!!
    assertNotNull(targetAlert, "Alert with key $alertKey not found")
    assertEquals(alertKey, targetAlert.alertId, "Alert ID mismatch for alert $alertKey")
    assertEquals(userId, targetAlert.userId, "User ID mismatch for alert $alertKey")

    val client = WebTestClient.bindToServer().baseUrl("http://localhost:9090").build()

    val testFuture =
        CompletableFuture.supplyAsync {
          client
              .get()
              .uri("/api/v1/alerts/stream?userId=$userId&stockSymbol=$symbol")
              .accept(MediaType.TEXT_EVENT_STREAM)
              .exchange()
              .expectStatus()
              .isOk
              .returnResult(String::class.java)
              .responseBody
              .takeUntil { eventString ->
                eventString.contains("\"alertId\":\"$alertKey\"") ||
                    eventString.contains(
                        "\"event\":\"keep-alive\"") // Optional: handle keep-alive events
              }
              //                .take(2, 1000)  // Take 2 events to ensure we don't miss our alert
              .timeout(Duration.ofSeconds(10))
              .collectList()
              .block()
        }

    // Publish JMS message after subscription
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window) { message ->
      message.setStringProperty("stockSymbol", symbol)
      message
    }

    val events =
        testFuture.get(10, TimeUnit.SECONDS)
            ?: fail("No SSE events received within timeout for alert $alertKey")

    // Find our specific alert in the received events
    val targetEvent =
        events
            .mapNotNull { eventString ->
              val json = eventString.substringAfter("data:").trim()
              try {
                val typeRef = object : TypeReference<TriggeredAlertEvent>() {}
                objectMapper.readValue(json, typeRef)
              } catch (e: Exception) {
                null
              }
            }
            .find { it.alert.alertId == alertKey }
            ?: fail("Alert $alertKey not found in received events: ${events.joinToString()}")

    // Verify the alert details
    assertEquals(targetAlert.alertId, targetEvent.alert.alertId, "Alert ID mismatch")
    assertEquals(userId, targetEvent.alert.userId, "User ID mismatch in event")
    assertEquals(symbol, targetEvent.alert.stockSymbol, "Symbol mismatch in event")

    val expectedPrice = expectedTriggeredPrice(window)
    priceComparison(expectedPrice, targetEvent.triggeredPrice)
  }

  private fun testNoAlertTriggered(
      symbol: String,
      userId: String,
      id: UUID,
      alertKey: String,
      window: AggregatedLHWindow
  ) {
    val testShard = shardManager.getShardForSymbol(symbol)
    assertNotNull(testShard, "Shard not found for symbol $symbol")

    val targetAlert = testShard!!.cache.getAlertDetails(id)!!
    assertNotNull(targetAlert, "Alert with key $alertKey not found")
    assertEquals(id, targetAlert.id, "Alert ID mismatch")

    val client = WebTestClient.bindToServer().baseUrl("http://localhost:9090").build()

    val testFuture =
        CompletableFuture.supplyAsync {
          client
              .get()
              .uri("/api/v1/alerts/stream?userId=$userId&stockSymbol=$symbol")
              .accept(MediaType.TEXT_EVENT_STREAM)
              .exchange()
              .expectStatus()
              .isOk
              .returnResult(String::class.java)
              .responseBody
              .take(1)
              .timeout(Duration.ofSeconds(5)) // Shorter timeout since we expect no events
              .blockFirst()
        }

    // Publish JMS message after subscription
    jmsTopicTemplate.convertAndSend(commonProperties.aggregatedUpdatesTopic, window) { message ->
      message.setStringProperty("stockSymbol", symbol)
      message
    }

    // Verify no alert was triggered
    assertThrows(Exception::class.java) { testFuture.get(5, TimeUnit.SECONDS) }
  }
}
