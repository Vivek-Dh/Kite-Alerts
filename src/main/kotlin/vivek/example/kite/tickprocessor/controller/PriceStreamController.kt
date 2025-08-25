package vivek.example.kite.tickprocessor.controller

import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.*
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow
import vivek.example.kite.tickprocessor.model.TickData
import vivek.example.kite.tickprocessor.service.PriceStreamService

@RestController
@RequestMapping("/api/v1/prices")
@CrossOrigin // Allow requests from any origin, useful for local UI development
class PriceStreamController(private val priceStreamService: PriceStreamService) {

  @GetMapping("/stream/aggregated", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
  fun streamAggregatedPrices(): Flow<ServerSentEvent<AggregatedLHWindow>> {
    return priceStreamService.aggregatedPriceUpdates.map { update ->
      ServerSentEvent.builder<AggregatedLHWindow>()
          .id("${update.symbol}-${update.windowEndTime}")
          .event("aggregated-price-update")
          .data(update)
          .retry(Duration.ofSeconds(10))
          .build()
    }
  }

  @GetMapping("/stream/raw/{symbol}", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
  fun streamRawTicks(@PathVariable symbol: String): Flow<ServerSentEvent<TickData>> {
    // Get the specific hot stream for the requested symbol
    return priceStreamService.getRawTickStream(symbol.uppercase()).map { tick ->
      ServerSentEvent.builder<TickData>()
          .id("${tick.symbol}-${tick.timestamp}")
          .event("raw-tick-update")
          .data(tick)
          .retry(Duration.ofSeconds(10))
          .build()
    }
  }
}
