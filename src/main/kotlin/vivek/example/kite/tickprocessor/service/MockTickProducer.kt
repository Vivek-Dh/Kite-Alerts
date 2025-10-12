package vivek.example.kite.tickprocessor.service

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import kotlin.random.Random
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.jms.core.JmsTemplate
import org.springframework.stereotype.Service
import vivek.example.kite.common.config.CommonProperties
import vivek.example.kite.tickprocessor.config.TickProcessorProperties
import vivek.example.kite.tickprocessor.model.TickData
import vivek.example.kite.tickprocessor.util.PriceSimulator

@Service
@Profile("!test")
class MockTickProducer(
    private val clock: Clock,
    @Qualifier("jmsTopicTemplate") private val jmsTopicTemplate: JmsTemplate,
    private val tickProcessorProperties: TickProcessorProperties,
    private val commonProperties: CommonProperties,
) {
  private val logger = LoggerFactory.getLogger(javaClass)
  private val producerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
  private val stockStateMap = mutableMapOf<String, StockState>()

  data class StockState(
      var currentPrice: BigDecimal,
      val minOverallPrice: BigDecimal,
      val maxOverallPrice: BigDecimal,
      var driftBias: Double,
      var ticksUntilDriftChange: Int
  )

  @PostConstruct
  fun init() {
    if (!tickProcessorProperties.mockProducer.enabled) {
      logger.info("MockTickProducer is disabled.")
      return
    }
    initializeStockStates()
    startProducers()
  }

  private fun initializeStockStates() {
    tickProcessorProperties.mockProducer.stockCategories.values.flatten().forEach { symbol ->
      val initialPrice = commonProperties.initialPrices[symbol] ?: BigDecimal.valueOf(500)
      stockStateMap[symbol] =
          StockState(
              currentPrice = initialPrice,
              minOverallPrice = initialPrice * BigDecimal("0.7"),
              maxOverallPrice = initialPrice * BigDecimal("1.3"),
              driftBias = 0.0,
              ticksUntilDriftChange = tickProcessorProperties.mockProducer.driftUpdateTicks)
    }
  }

  private fun startProducers() {
    tickProcessorProperties.mockProducer.stockCategories.forEach { (category, symbols) ->
      val frequency = tickProcessorProperties.mockProducer.frequencyMillis[category]
      if (frequency == null) {
        logger.warn("No frequency defined for category $category. Skipping.")
        return@forEach
      }
      symbols.forEach { symbol ->
        producerScope.launch { produceTicksForSymbol(symbol, category, frequency) }
      }
    }
  }

  private suspend fun produceTicksForSymbol(symbol: String, category: String, frequency: Long) {
    val state = stockStateMap[symbol] ?: return
    val props = tickProcessorProperties.mockProducer
    val topicName = getTopicForCategory(category)

    var tickCounter = 0L

    logger.info(
        "Starting tick generation for $symbol on topic $topicName with frequency ${frequency}ms")

    while (currentCoroutineContext().isActive) {
      // Update drift bias if needed
      if (state.ticksUntilDriftChange-- <= 0) {
        state.driftBias = (Random.nextDouble() * 2 - 1) * props.driftBiasRange
        state.ticksUntilDriftChange = props.driftUpdateTicks
        logger.debug("Updating drift for $symbol to ${state.driftBias}")
      }

      // Calculate next price using the extracted utility function
      state.currentPrice =
          PriceSimulator.calculateNextPrice(
              currentPrice = state.currentPrice,
              minOverallPrice = state.minOverallPrice,
              maxOverallPrice = state.maxOverallPrice,
              driftBias = state.driftBias,
              tickVolatility = props.tickVolatility,
              randomShock = (Random.nextDouble() * 2 - 1) // Provide a random shock
              )

      val tickId = "${symbol}-${++tickCounter}-${Instant.now(clock).toEpochMilli()}"
      val tick =
          TickData(
              id = tickId,
              symbol = symbol,
              price = state.currentPrice,
              timestamp = Instant.now().toEpochMilli(),
              volume = Random.nextLong(100, 10000))

      logger.debug("Producing tick: id={}, symbol={}, price={}", tickId, symbol, state.currentPrice)

      // Send to JMS Topic with JMSXGroupID
      jmsTopicTemplate.convertAndSend(topicName, tick) { message ->
        message.setStringProperty("JMSXGroupID", symbol)
        message.setStringProperty("TICK_ID", tickId)
        message
      }
      delay(frequency)
    }
  }

  private fun getTopicForCategory(category: String): String {
    return when (category) {
      "HIGH_ACTIVITY" -> tickProcessorProperties.jms.topics.rawTicksHighActivity
      "MEDIUM_ACTIVITY" -> tickProcessorProperties.jms.topics.rawTicksMediumActivity
      "LOW_ACTIVITY" -> tickProcessorProperties.jms.topics.rawTicksLowActivity
      else -> throw IllegalArgumentException("Unknown category: $category")
    }
  }

  @PreDestroy
  fun shutdown() {
    logger.info("Shutting down MockTickProducer...")
    producerScope.cancel()
  }
}
