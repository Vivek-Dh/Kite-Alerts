package vivek.example.kite.tickprocessor.service

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.jms.annotation.JmsListener
import org.springframework.stereotype.Service
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow
import vivek.example.kite.tickprocessor.model.TickData

@Service
class PriceStreamService {
  private val logger = LoggerFactory.getLogger(javaClass)

  // A hot flow for aggregated price updates
  private val _aggregatedPriceUpdates = MutableSharedFlow<AggregatedLHWindow>(replay = 1)
  val aggregatedPriceUpdates = _aggregatedPriceUpdates.asSharedFlow()

  // A map to hold hot flows for raw ticks, one for each symbol
  private val rawTickFlows = ConcurrentHashMap<String, MutableSharedFlow<TickData>>()

  /**
   * Returns a hot stream (SharedFlow) of raw ticks for a specific symbol. Creates the flow
   * on-demand if it doesn't exist.
   */
  fun getRawTickStream(symbol: String): Flow<TickData> {
    return rawTickFlows
        .computeIfAbsent(symbol) {
          logger.info("Creating new raw tick stream for symbol: $symbol")
          // Create a new hot flow that replays the last 10 ticks to new subscribers
          MutableSharedFlow(replay = 10)
        }
        .asSharedFlow()
  }

  // Listener for aggregated data, feeding the aggregated stream
  @JmsListener(
      destination = "\${tick-processor.jms.topics.aggregatedUpdates}",
      containerFactory = "priceStreamListenerFactory")
  fun receiveAggregatedUpdate(update: AggregatedLHWindow) {
    runBlocking { _aggregatedPriceUpdates.emit(update) }
  }

  // Listeners for raw tick data, feeding the individual raw tick streams
  @JmsListener(
      destination = "\${tick-processor.jms.topics.rawTicksHighActivity}",
      containerFactory = "highActivityFactory")
  fun receiveHighActivityRawTick(tick: TickData) {
    processRawTick(tick)
  }

  @JmsListener(
      destination = "\${tick-processor.jms.topics.rawTicksMediumActivity}",
      containerFactory = "mediumActivityFactory")
  fun receiveMediumActivityRawTick(tick: TickData) {
    processRawTick(tick)
  }

  @JmsListener(
      destination = "\${tick-processor.jms.topics.rawTicksLowActivity}",
      containerFactory = "lowActivityFactory")
  fun receiveLowActivityRawTick(tick: TickData) {
    processRawTick(tick)
  }

  private fun processRawTick(tick: TickData) {
    // Find the specific flow for this symbol and emit the tick into it
    rawTickFlows[tick.symbol]?.let { flow -> runBlocking { flow.emit(tick) } }
  }
}
