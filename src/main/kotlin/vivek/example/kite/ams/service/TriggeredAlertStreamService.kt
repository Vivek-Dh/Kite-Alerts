package vivek.example.kite.ams.service

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.jms.annotation.JmsListener
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.RequestParam
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.repository.AlertRepository
import vivek.example.kite.ams.shard.ShardManager
import vivek.example.kite.common.config.CommonProperties

@Service
class TriggeredAlertStreamService(
    private val commonProperties: CommonProperties,
    private val alertRepository: AlertRepository,
    private val shardManager: ShardManager
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  // A single hot flow to broadcast all triggered alerts
  private val _triggeredAlerts = MutableSharedFlow<TriggeredAlertEvent>(replay = 10)
  val triggeredAlerts = _triggeredAlerts.asSharedFlow()

  fun getActiveAlerts(
      @RequestParam userId: String?,
      @RequestParam stockSymbol: String?
  ): List<Alert> {
    val symbols: List<String> = stockSymbol?.let { listOf(it) } ?: commonProperties.stocks
    val symbolAlerts: List<Alert> =
        symbols.flatMap { symbol ->
          val shard = shardManager.getShardForSymbol(symbol)
          if (shard != null) {
            return@flatMap shard.cache.getActiveAlerts()
          }
          return emptyList()
        }
    return userId?.let { symbolAlerts.filter { it.userId == userId } } ?: symbolAlerts
  }

  @JmsListener(
      destination = "\${common.triggeredAlertsTopic}",
      containerFactory = "amsListenerContainerFactory" // Can reuse the generic AMS factory
      )
  fun receiveTriggeredAlert(event: TriggeredAlertEvent) {
    logger.info("SSE Service received triggered alert: {}", event.alert.alertId)
    // Emit the event into the hot stream for any connected UI clients
    runBlocking { _triggeredAlerts.emit(event) }
  }
}
