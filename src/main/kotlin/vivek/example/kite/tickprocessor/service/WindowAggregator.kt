package vivek.example.kite.tickprocessor.service

import jakarta.annotation.PreDestroy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jms.annotation.JmsListener
import org.springframework.jms.core.JmsTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import vivek.example.kite.tickprocessor.config.TickProcessorProperties
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow
import vivek.example.kite.tickprocessor.model.TickData

@Service
class WindowAggregator(
    @Qualifier("jmsTopicTemplate") private val jmsTopicTemplate: JmsTemplate,
    private val tickProcessorProperties: TickProcessorProperties,
    private val taskExecutor: Executor
) {
  private val logger = LoggerFactory.getLogger(javaClass)
  private val tickBuffers = ConcurrentHashMap<String, MutableList<TickData>>()
  private val windowStartTimes = ConcurrentHashMap<String, Long>()

  @JmsListener(
      destination = "\${tick-processor.jms.topics.rawTicksHighActivity}",
      containerFactory = "highActivityFactory",
      subscription = "highActivitySubscription")
  fun handleHighActivityTick(tick: TickData) {
    processTick(tick, "HIGH_ACTIVITY")
  }

  @JmsListener(
      destination = "\${tick-processor.jms.topics.rawTicksMediumActivity}",
      containerFactory = "mediumActivityFactory",
      subscription = "mediumActivitySubscription")
  fun handleMediumActivityTick(tick: TickData) {
    processTick(tick, "MEDIUM_ACTIVITY")
  }

  @JmsListener(
      destination = "\${tick-processor.jms.topics.rawTicksLowActivity}",
      containerFactory = "lowActivityFactory",
      subscription = "lowActivitySubscription")
  fun handleLowActivityTick(tick: TickData) {
    processTick(tick, "LOW_ACTIVITY")
  }

  private fun processTick(tick: TickData, category: String) {
    logger.debug(
        "Processing tick: id={}, symbol={}, price={}, time={}",
        tick.id,
        tick.symbol,
        tick.price,
        tick.timestamp)

    val buffer = tickBuffers.computeIfAbsent(tick.symbol) { CopyOnWriteArrayList() }

    // Check for duplicates with more detailed logging
    val existing = buffer.find { it.id == tick.id }
    if (existing != null) {
      logger.warn("Duplicate tick detected! id={}, existing={}, new={}", tick.id, existing, tick)
      return
    }

    buffer.add(tick)
    windowStartTimes.putIfAbsent(tick.symbol, System.currentTimeMillis())
    logger.debug("Added tick to buffer: {}", tick)
  }

  @Scheduled(fixedRateString = "\${tick-processor.windowAggregator.timerCheckIntervalMillis}")
  fun processWindows() {
    val currentTime = System.currentTimeMillis()
    val symbolsToProcess = windowStartTimes.keys.toList() // Take a snapshot of current symbols

    symbolsToProcess.forEach { symbol ->
      taskExecutor.execute { processSymbolWindows(symbol, currentTime) }
    }
  }

  private fun processSymbolWindows(symbol: String, currentTime: Long) {
    val category = getCategoryForSymbol(symbol) ?: return
    val windowDuration =
        tickProcessorProperties.windowAggregator.windowDurationMillis[category] ?: return

    // Get the buffer and start time atomically
    val buffer = tickBuffers[symbol] ?: return
    val startTime = windowStartTimes[symbol] ?: return

    // Check if window should be closed
    if (currentTime - startTime >= windowDuration) {
      val ticksForWindow =
          synchronized(buffer) {
            val ticks = ArrayList(buffer)
            buffer.clear()
            ticks
          }

      // Only process if we have ticks
      if (ticksForWindow.isNotEmpty()) {
        aggregateAndPublish(symbol, startTime, currentTime, ticksForWindow)
      }

      // Always update the window start time if we're still getting ticks
      if (tickBuffers[symbol]?.isNotEmpty() == true) {
        windowStartTimes[symbol] = currentTime
      } else {
        // Clean up if no more ticks
        tickBuffers.remove(symbol)
        windowStartTimes.remove(symbol)
      }
    }
  }

  private fun aggregateAndPublish(
      symbol: String,
      startTime: Long,
      endTime: Long,
      ticks: List<TickData>
  ) {
    val prices = ticks.map { it.price }
    val low = prices.minOrNull() ?: return
    val high = prices.maxOrNull() ?: return

    val aggregatedWindow =
        AggregatedLHWindow(
            symbol = symbol,
            windowStartTime = startTime,
            windowEndTime = endTime,
            low = low,
            high = high,
            ticksCount = ticks.size)

    jmsTopicTemplate.convertAndSend(
        tickProcessorProperties.jms.topics.aggregatedUpdates, aggregatedWindow) { message ->
          message.setStringProperty("stockSymbol", symbol)
          message
        }
    logger.info("Published aggregated window for $symbol: L=$low, H=$high over ${ticks.size} ticks")
  }

  private fun getCategoryForSymbol(symbol: String): String? {
    return tickProcessorProperties.mockProducer.stockCategories.entries
        .find { it.value.contains(symbol) }
        ?.key
  }

  @PreDestroy
  fun shutdown() {
    logger.info("Shutting down WindowAggregator...")
  }
}
