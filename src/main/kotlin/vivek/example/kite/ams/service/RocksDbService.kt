package vivek.example.kite.ams.service

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import vivek.example.kite.ams.config.RocksDbConfig
import vivek.example.kite.ams.model.Alert

@Service
class RocksDbService(
    private val rocksDbConfig: RocksDbConfig,
    private val objectMapper: ObjectMapper
) {
  private val logger = LoggerFactory.getLogger(javaClass)
  private val dbs = ConcurrentHashMap<String, RocksDB>()

  companion object {
    private val ALERT_PREFIX = "alert::".toByteArray(StandardCharsets.UTF_8)

    fun getAlertKey(alertId: UUID): ByteArray =
        "alert::$alertId".toByteArray(StandardCharsets.UTF_8)

    fun getSymbolIndexPrefix(symbol: String): ByteArray =
        "symbol-idx::$symbol::".toByteArray(StandardCharsets.UTF_8)

    fun getSymbolIndexKey(symbol: String, alertId: UUID): ByteArray =
        "symbol-idx::$symbol::$alertId".toByteArray(StandardCharsets.UTF_8)
  }

  fun getDb(shardName: String): RocksDB {
    return dbs.computeIfAbsent(shardName) {
      val dbPath = File(rocksDbConfig.basePath, shardName)
      dbPath.mkdirs()
      val options = Options().setCreateIfMissing(true)
      logger.info("Opening RocksDB for shard {} at {}", shardName, dbPath.absolutePath)
      RocksDB.open(options, dbPath.absolutePath)
    }
  }

  fun saveAlert(shardName: String, alert: Alert) {
    val db = getDb(shardName)
    val alertKey = getAlertKey(alert.id)
    val alertValue = objectMapper.writeValueAsBytes(alert)
    val indexKey = getSymbolIndexKey(alert.stockSymbol, alert.id)

    db.put(alertKey, alertValue)
    db.put(indexKey, alertKey)
  }

  fun deleteAlert(shardName: String, alert: Alert) {
    val db = getDb(shardName)
    val alertKey = getAlertKey(alert.id)
    val indexKey = getSymbolIndexKey(alert.stockSymbol, alert.id)
    db.delete(indexKey)
    db.delete(alertKey)
    logger.info("[{}] Deleted alert {} from RocksDB.", shardName, alert.getLogKey())
  }

  fun getAlert(shardName: String, alertId: UUID): Alert? {
    val db = getDb(shardName)
    val alertKey = getAlertKey(alertId)
    val alertBytes = db.get(alertKey)
    return if (alertBytes != null) objectMapper.readValue(alertBytes, Alert::class.java) else null
  }

  fun getAllAlerts(shardName: String): List<Alert> {
    val db = getDb(shardName)
    val alerts = mutableListOf<Alert>()
    db.newIterator().use { iter ->
      iter.seek(ALERT_PREFIX)
      while (iter.isValid && iter.key().startsWith(ALERT_PREFIX)) {
        try {
          alerts.add(objectMapper.readValue(iter.value(), Alert::class.java))
        } catch (e: Exception) {
          logger.error("Failed to deserialize alert during scan for shard {}", shardName, e)
        }
        iter.next()
      }
    }
    return alerts
  }

  @PreDestroy
  fun closeAll() {
    logger.info("Closing all RocksDB instances...")
    dbs.values.forEach { it.close() }
    dbs.clear()
  }

  private fun ByteArray.startsWith(prefix: ByteArray): Boolean {
    if (prefix.size > this.size) return false
    for (i in prefix.indices) {
      if (this[i] != prefix[i]) return false
    }
    return true
  }
}
