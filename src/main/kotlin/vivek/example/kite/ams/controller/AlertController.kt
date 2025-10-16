package vivek.example.kite.ams.controller

import java.time.Duration
import java.util.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.springframework.http.CacheControl
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.*
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.repository.PostgresAlertRepository
import vivek.example.kite.ams.service.AlertManagementService
import vivek.example.kite.ams.service.RocksDbService
import vivek.example.kite.ams.service.TriggeredAlertStreamService
import vivek.example.kite.ams.shard.ShardManager

@RestController
@RequestMapping("/api/v1/alerts")
@CrossOrigin // Allow requests from any origin
class AlertController(
    private val streamService: TriggeredAlertStreamService,
    private val shardManager: ShardManager,
    private val alertManagementService: AlertManagementService,
    private val rocksDbService: RocksDbService,
    private val postgresAlertRepository: PostgresAlertRepository
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
      ResponseEntity.ok(alertManagementService.createOrUpdateAlert(alert))
    } catch (e: Exception) {
      ResponseEntity.badRequest().body(e.message)
    }
  }

  @GetMapping("/rocksdb/{shardName}/{alertId}")
  fun getAlertFromRocksDb(
      @PathVariable shardName: String,
      @PathVariable alertId: String
  ): ResponseEntity<*> {
    val alert = rocksDbService.getAlert(shardName, alertId)
    return if (alert != null) {
      ResponseEntity.ok(alert)
    } else {
      ResponseEntity.notFound().build<Alert>()
    }
  }

  @GetMapping("/postgres/{alertId}")
  fun getAlertFromPostgres(@PathVariable alertId: UUID): ResponseEntity<*> {
    val alert = postgresAlertRepository.findById(alertId)
    return if (alert.isPresent) {
      ResponseEntity.ok(alert.get())
    } else {
      ResponseEntity.notFound().build<Alert>()
    }
  }
}
