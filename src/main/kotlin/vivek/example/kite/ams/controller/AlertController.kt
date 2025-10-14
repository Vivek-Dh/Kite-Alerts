package vivek.example.kite.ams.controller

import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.springframework.http.CacheControl
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.service.TriggeredAlertStreamService
import vivek.example.kite.ams.shard.ShardManager

@RestController
@RequestMapping("/api/v1/alerts")
@CrossOrigin // Allow requests from any origin
class AlertController(
    private val streamService: TriggeredAlertStreamService,
    private val shardManager: ShardManager
) {

  @GetMapping("/active")
  fun streamConfiguredAlerts(
      @RequestParam(required = false) userId: String?,
      @RequestParam(required = false) stockSymbol: String?
  ): ResponseEntity<*> {
    return ResponseEntity.ok()
        .cacheControl(CacheControl.noCache())
        .body(streamService.getActiveAlerts(userId, stockSymbol))
  }

  @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
  fun streamTriggeredAlerts(
      @RequestParam(required = false) userId: String?,
      @RequestParam(required = false) stockSymbol: String?
  ): Flow<ServerSentEvent<TriggeredAlertEvent>> {
    return streamService.triggeredAlerts
        .filter { event ->
          // Apply filters if provided
          val userMatches = userId == null || event.alert.userId.equals(userId, ignoreCase = true)
          val symbolMatches =
              stockSymbol == null || event.alert.stockSymbol.equals(stockSymbol, ignoreCase = true)
          userMatches && symbolMatches
        }
        .map { event ->
          ServerSentEvent.builder<TriggeredAlertEvent>()
              .id(event.eventId)
              .event("triggered-alert")
              .data(event)
              .retry(Duration.ofSeconds(10))
              .build()
        }
  }

  @PostMapping(
      "/create",
      consumes = [MediaType.APPLICATION_JSON_VALUE],
      produces = [MediaType.APPLICATION_JSON_VALUE])
  fun createAlert(@RequestBody alert: AlertRequest): ResponseEntity<*> {
    return try {
      ResponseEntity.ok(shardManager.createAlert(alert))
    } catch (e: Exception) {
      ResponseEntity.badRequest().body(e.message)
    }
  }
}
