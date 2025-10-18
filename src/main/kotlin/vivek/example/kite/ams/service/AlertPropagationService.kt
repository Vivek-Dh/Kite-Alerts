package vivek.example.kite.ams.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.jms.annotation.JmsListener
import org.springframework.stereotype.Service
import vivek.example.kite.ams.model.AlertChangeEvent
import vivek.example.kite.ams.shard.ShardManager

@Service
class AlertPropagationService(
    private val shardManager: ShardManager,
    private val rocksDbService: RocksDbService,
    private val objectMapper: ObjectMapper
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  @JmsListener(
      destination = "\${common.alertDefinitionUpdatesTopic}",
      containerFactory = "amsListenerContainerFactory")
  fun processAlertChange(message: String) {
    // If any operation below fails, the exception will propagate up to the
    // Spring JMS listener container. Because we are in CLIENT_ACKNOWLEDGE mode,
    // the container will NOT acknowledge the message, and it will be redelivered
    // by the broker according to its redelivery policy. This is the correct
    // behavior to ensure "at-least-once" processing and data consistency.
    val event = objectMapper.readValue<AlertChangeEvent>(message)
    val alert = event.alert
    val shard = shardManager.getShardForSymbol(alert.stockSymbol)

    if (shard == null) {
      logger.warn(
          "Received alert change {} for symbol {} which is not assigned to any shard. Ignoring.",
          alert.getLogKey(),
          alert.stockSymbol)
      // This is a valid terminal state, so we don't throw an exception.
      // The message should be acknowledged and discarded.
      return
    }

    logger.info(
        "Processing alert change event {},  {} for alertId {} on shard {}",
        event.eventType,
        alert.getLogKey(),
        alert.id,
        shard.name)

    // Update L2 Cache (RocksDB) based on the event type
    when (event.eventType) {
      "CREATE",
      "UPDATE" -> rocksDbService.saveAlert(shard.name, alert)
      "DELETE" -> rocksDbService.deleteAlert(shard.name, alert)
    }

    // Update L1 Cache (In-Memory). This is also idempotent.
    shard.cache.addOrUpdateAlert(alert)

    logger.debug("Updated L1 and L2 caches for alertId {}", alert.id)
  }
}
