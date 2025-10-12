package vivek.example.kite

import jakarta.jms.Connection
import jakarta.jms.MessageListener
import jakarta.jms.Session
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jms.core.JmsTemplate
import org.springframework.jms.support.converter.MessageConverter
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import vivek.example.kite.ams.shard.AmsTestConfig
import vivek.example.kite.tickprocessor.config.TickProcessorProperties
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow
import vivek.example.kite.tickprocessor.model.TickData

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(locations = ["classpath:application-test.yml"])
@Import(AmsTestConfig::class)
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class JmsTopicIntegrationTests {

  private val logger = LoggerFactory.getLogger(javaClass)

  @Autowired private lateinit var clock: Clock

  @Autowired @Qualifier("jmsTopicTemplate") private lateinit var jmsTopicTemplate: JmsTemplate

  @Autowired private lateinit var tickProcessorProperties: TickProcessorProperties

  @Autowired private lateinit var messageConverter: MessageConverter

  @Test
  @Order(1)
  fun `should publish a message to a raw tick topic and consume it successfully`() {
    val testTopic = tickProcessorProperties.jms.topics.rawTicksHighActivity
    val testTick = createTestTick("reliance-2500.50", "2500.50")
    val receivedFuture = CompletableFuture<TickData>()

    withConnection { connection ->
      withSession(connection) { session ->
        createConsumer(session, testTopic) { receivedFuture.complete(it) }
            .use {
              logger.info("Publishing test tick: $testTick")
              jmsTopicTemplate.convertAndSend(testTopic, testTick)

              val received = receivedFuture.get(5, TimeUnit.SECONDS)
              verifyTick(testTick, received)
              logger.info("Successfully verified the published message was consumed.")
            }
      }
    }
  }

  @Test
  @Order(2)
  fun `late-joining consumer should miss messages sent before it subscribed`() {
    val testTopic = tickProcessorProperties.jms.topics.rawTicksHighActivity
    val tick1 = createTestTick("reliance-2500.50", "2500.50")
    val tick2 = createTestTick("reliance-2501.00", "2501.00", 2100)

    val amsFuture1 = CompletableFuture<TickData>()
    val amsFuture2 = CompletableFuture<TickData>()
    val uiFuture1 = CompletableFuture<TickData>()
    val uiFuture2 = CompletableFuture<TickData>()

    withConnection { connection ->
      withSession(connection) { session ->
        // First consumer (AMS)
        createConsumer(session, testTopic) { tick ->
              when (tick.id) {
                tick1.id -> amsFuture1.complete(tick)
                tick2.id -> amsFuture2.complete(tick)
              }
            }
            .use {
              // Send first tick (only AMS should get it)
              logger.info("Publishing tick 1 (only AMS should receive this)")
              jmsTopicTemplate.convertAndSend(testTopic, tick1)
              verifyTick(tick1, amsFuture1.get(5, TimeUnit.SECONDS))

              // Wait and create second consumer (UI)
              logger.info("Waiting before creating UI consumer...")
              Thread.sleep(2000)

              createConsumer(session, testTopic) { tick ->
                    when (tick.id) {
                      tick1.id -> uiFuture1.complete(tick)
                      tick2.id -> uiFuture2.complete(tick)
                    }
                  }
                  .use {
                    // Send second tick (both should get it)
                    logger.info("Publishing tick 2 (both consumers should receive this)")
                    jmsTopicTemplate.convertAndSend(testTopic, tick2)

                    // Verify AMS got both ticks
                    verifyTick(tick2, amsFuture2.get(5, TimeUnit.SECONDS))

                    // Verify UI only got the second tick
                    verifyTick(tick2, uiFuture2.get(5, TimeUnit.SECONDS))

                    // Verify UI didn't get the first tick
                    assertThrows(TimeoutException::class.java) {
                      uiFuture1.get(5, TimeUnit.SECONDS)
                    }
                    logger.info("Verified UI consumer did not receive tick 1")
                  }
            }
      }
    }
  }

  @Test
  @Order(3)
  fun `duplicate messages consumed by topic should be ignored for processing`() {
    val testTopic = tickProcessorProperties.jms.topics.rawTicksHighActivity
    val symbol = "RELIANCE"
    val tickId = "$symbol-1-${Instant.now(clock).toEpochMilli()}"
    val price = "2500.50"
    val timestamp = Instant.now(clock).toEpochMilli()

    val tick =
        TickData(
            id = tickId,
            symbol = symbol,
            price = BigDecimal(price),
            timestamp = timestamp,
            volume = 1000)

    // Create a latch to wait for aggregation
    val latch = CountDownLatch(1)
    val aggregatedData = AtomicReference<AggregatedLHWindow>()

    // Subscribe to aggregated updates FIRST
    withConnection { connection ->
      withSession(connection) { session ->
        val consumer =
            session.createConsumer(
                session.createTopic(tickProcessorProperties.jms.topics.aggregatedUpdates),
                "stockSymbol = '$symbol'" // Changed from "symbol" to "stockSymbol"
                )

        consumer.messageListener = MessageListener { message ->
          logger.info("Received message: $message")
          try {
            val data = messageConverter.fromMessage(message) as? AggregatedLHWindow
            logger.info("Deserialized data: $data")
            if (data?.symbol == symbol) {
              logger.info("Matching symbol found, setting data")
              aggregatedData.set(data)
              latch.countDown()
            }
          } catch (e: Exception) {
            logger.error("Error processing message", e)
          }
        }

        // Give the consumer time to subscribe
        Thread.sleep(1000)

        logger.info("Sending first tick")
        // Send the same tick multiple times
        repeat(3) { i ->
          jmsTopicTemplate.convertAndSend(testTopic, tick) { message ->
            message.setStringProperty("JMSXGroupID", symbol)
            message.setStringProperty("TICK_ID", tickId)
            message
          }
          logger.info("Sent tick ${i + 1}")
          Thread.sleep(100) // Small delay between sends
        }

        // Wait for aggregation with longer timeout
        logger.info("Waiting for aggregated data...")
        val received = latch.await(10, TimeUnit.SECONDS)
        logger.info("Latch received: $received, Data: ${aggregatedData.get()}")

        // Verify the results
        val result = aggregatedData.get()
        assertNotNull(result, "No aggregated data received")
        assertEquals(symbol, result.symbol, "Symbol mismatch")
        assertEquals(1, result.ticksCount, "Expected only one tick to be processed")
        assertEquals(0, BigDecimal(price).compareTo(result.high), "High price mismatch")
        assertEquals(0, BigDecimal(price).compareTo(result.low), "Low price mismatch")

        consumer.close()
      }
    }
  }

  private fun createTestTick(id: String, price: String, timeOffset: Long = 0): TickData {
    return TickData(
        id = id,
        symbol = "RELIANCE",
        price = BigDecimal(price),
        timestamp = Instant.now(clock).toEpochMilli() + timeOffset,
        volume = 100)
  }

  private fun withConnection(block: (Connection) -> Unit) {
    jmsTopicTemplate.connectionFactory!!.createConnection().use { connection ->
      connection.start()
      logger.info("Test connection started.")
      block(connection)
    }
  }

  private fun withSession(connection: Connection, block: (Session) -> Unit) {
    connection.createSession(false, Session.AUTO_ACKNOWLEDGE).use { session -> block(session) }
  }

  private fun createConsumer(
      session: Session,
      topicName: String,
      messageHandler: (TickData) -> Unit
  ) =
      session.createConsumer(session.createTopic(topicName)).apply {
        messageListener = MessageListener { message ->
          val tick = messageConverter.fromMessage(message) as TickData
          // Corrected the log statement to remove the null messageSelector
          logger.info("Consumer for topic '$topicName' received tick: $tick")
          messageHandler(tick)
        }
        logger.info("Consumer is now listening on topic: $topicName")
      }

  private fun verifyTick(expected: TickData, actual: TickData) {
    assertEquals(expected.symbol, actual.symbol, "Symbol mismatch")
    assertEquals(0, expected.price.compareTo(actual.price), "Price mismatch")
    assertEquals(expected.timestamp, actual.timestamp, "Timestamp mismatch")
    assertEquals(expected.volume, actual.volume, "Volume mismatch")
  }
}
