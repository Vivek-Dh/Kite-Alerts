package vivek.example.kite

import jakarta.jms.Connection
import jakarta.jms.MessageListener
import jakarta.jms.Session
import java.math.BigDecimal
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jms.core.JmsTemplate
import org.springframework.jms.support.converter.MessageConverter
import org.springframework.test.context.ActiveProfiles
import vivek.example.kite.config.AppProperties
import vivek.example.kite.tickprocessor.model.TickData

@SpringBootTest
@ActiveProfiles("test")
class JmsTopicIntegrationTests {

  private val logger = LoggerFactory.getLogger(javaClass)

  @Autowired @Qualifier("jmsTopicTemplate") private lateinit var jmsTopicTemplate: JmsTemplate

  @Autowired private lateinit var appProperties: AppProperties

  @Autowired private lateinit var messageConverter: MessageConverter

  @Test
  fun `should publish a message to a raw tick topic and consume it successfully`() {
    val testTopic = appProperties.jms.topics.rawTicksHighActivity
    val testTick = createTestTick("2500.50")
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
  fun `late-joining consumer should miss messages sent before it subscribed`() {
    val testTopic = appProperties.jms.topics.rawTicksHighActivity
    val tick1 = createTestTick("2500.50")
    val tick2 = createTestTick("2501.00", 2100)

    val amsFuture1 = CompletableFuture<TickData>()
    val amsFuture2 = CompletableFuture<TickData>()
    val uiFuture1 = CompletableFuture<TickData>()
    val uiFuture2 = CompletableFuture<TickData>()

    withConnection { connection ->
      withSession(connection) { session ->
        // First consumer (AMS)
        createConsumer(session, testTopic) { tick ->
              when (tick.timestamp) {
                tick1.timestamp -> amsFuture1.complete(tick)
                tick2.timestamp -> amsFuture2.complete(tick)
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
                    if (tick.timestamp == tick2.timestamp) {
                      uiFuture2.complete(tick)
                    } else {
                      uiFuture1.complete(tick)
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
                      uiFuture1.get(1, TimeUnit.SECONDS)
                    }
                    logger.info("Verified UI consumer did not receive tick 1")
                  }
            }
      }
    }
  }

  private fun createTestTick(price: String, timeOffset: Long = 0): TickData {
    return TickData(
        symbol = "RELIANCE",
        price = BigDecimal(price),
        timestamp = System.currentTimeMillis() + timeOffset,
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
          logger.info("Consumer received tick: $tick")
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
