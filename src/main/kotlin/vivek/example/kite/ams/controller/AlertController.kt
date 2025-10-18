package vivek.example.kite.ams.controller

import java.time.Duration
import java.util.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import org.slf4j.LoggerFactory
import org.springframework.http.CacheControl
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.*
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.repository.TriggeredAlertHistoryRepository
import vivek.example.kite.ams.service.AlertAlreadyExistsException
import vivek.example.kite.ams.service.AlertManagementService
import vivek.example.kite.ams.service.InvalidPriceException
import vivek.example.kite.ams.service.InvalidStockException
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
    private val historyRepository: TriggeredAlertHistoryRepository
) {

  private val logger = LoggerFactory.getLogger(javaClass)

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
  fun createAlert(@RequestBody alertRequest: AlertRequest): ResponseEntity<*> {
    return try {
      return ResponseEntity.ok(alertManagementService.createAlert(alertRequest))
    } catch (e: Exception) {
      ResponseEntity.badRequest().body(e.message)
    }
  }

  @DeleteMapping("/delete/{alertId}", produces = [MediaType.APPLICATION_JSON_VALUE])
  fun deleteAlert(@PathVariable alertId: UUID): ResponseEntity<*> {
    return ResponseEntity.ok(alertManagementService.deactivateAlert(alertId))
  }

  // New endpoint to get triggered alert history for a user
  @GetMapping("/history/{userId}")
  fun getTriggeredAlertHistory(@PathVariable userId: String): ResponseEntity<*> {
    return ResponseEntity.ok(historyRepository.findByUserId(userId))
  }

  // --- DEBUG ENDPOINTS ---

  @GetMapping("/postgres/{alertId}")
  fun getAlertFromPostgres(@PathVariable alertId: String): ResponseEntity<*> {
    val alert = alertManagementService.findAlertById(UUID.fromString(alertId))
    return if (alert != null) ResponseEntity.ok(alert) else ResponseEntity.notFound().build<Alert>()
  }

  @GetMapping("/rocksdb/{shardName}/{alertId}")
  fun getAlertFromRocksDb(
      @PathVariable shardName: String,
      @PathVariable alertId: String
  ): ResponseEntity<*> {
    val alert = rocksDbService.getAlert(shardName, UUID.fromString(alertId))
    return if (alert != null) ResponseEntity.ok(alert) else ResponseEntity.notFound().build<Alert>()
  }

  @GetMapping("/rocksdb/{shardName}/all")
  fun getAllAlertsFromRocksDb(@PathVariable shardName: String): ResponseEntity<*> {
    return ResponseEntity.ok(rocksDbService.getAllAlerts(shardName))
  }

  @ExceptionHandler(AlertAlreadyExistsException::class)
  fun handleAlertAlreadyExists(ex: AlertAlreadyExistsException): ResponseEntity<ProblemDetail> {
    val problemDetail =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST, ex.message ?: "Alert already exists")
    problemDetail.title = "Alert Already Exists"
    problemDetail.setProperty("errorCode", "ALREADY_EXISTS")

    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(problemDetail)
  }

  @ExceptionHandler(InvalidStockException::class)
  fun handleInvalidStock(ex: InvalidStockException): ResponseEntity<ProblemDetail> {
    val problemDetail =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST, ex.message ?: "Invalid stock symbol")
    problemDetail.title = "Invalid Stock Symbol"
    problemDetail.setProperty("errorCode", "INVALID_STOCK")

    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(problemDetail)
  }

  @ExceptionHandler(InvalidPriceException::class)
  fun handleInvalidPrice(ex: InvalidPriceException): ResponseEntity<ProblemDetail> {
    val problemDetail =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST, ex.message ?: "Invalid price threshold")
    problemDetail.title = "Invalid Price Threshold"
    problemDetail.setProperty("errorCode", "INVALID_PRICE")

    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(problemDetail)
  }
}
