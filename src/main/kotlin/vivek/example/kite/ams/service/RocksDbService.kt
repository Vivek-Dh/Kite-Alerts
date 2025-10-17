package vivek.example.kite.ams.service

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import java.io.File
import java.nio.charset.StandardCharsets
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
    private const val SYMBOL_INDEX_PREFIX = "symbol-idx::"

    fun getSymbolIndexPrefix(symbol: String): ByteArray {
      return "$SYMBOL_INDEX_PREFIX$symbol::".toByteArray(StandardCharsets.UTF_8)
    }
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
    val alertIdBytes = alert.id.toString().toByteArray(StandardCharsets.UTF_8)
    val alertJsonBytes = objectMapper.writeValueAsBytes(alert)

    // Primary key write
    db.put(alertIdBytes, alertJsonBytes)

    // Secondary index write (symbol -> alertId)
    val symbolIndexKey = getSymbolIndexKey(alert.stockSymbol, alert.id.toString())
    db.put(symbolIndexKey, alertIdBytes)
  }

  fun getAlert(shardName: String, alertId: String): Alert? {
    val db = getDb(shardName)
    val alertBytes = db.get(alertId.toByteArray(StandardCharsets.UTF_8))
    return if (alertBytes != null) {
      objectMapper.readValue(alertBytes, Alert::class.java)
    } else {
      null
    }
  }

  private fun getSymbolIndexKey(symbol: String, alertId: String): ByteArray {
    return "$SYMBOL_INDEX_PREFIX$symbol::$alertId".toByteArray(StandardCharsets.UTF_8)
  }

  @PreDestroy
  fun closeAll() {
    logger.info("Closing all RocksDB instances...")
    dbs.values.forEach { it.close() }
    dbs.clear()
    // Delete directories if test
    if (rocksDbConfig.basePath.contains("kite_alerts_test")) {
      val dbPath = File(rocksDbConfig.basePath)
      // delete all directories inside
      dbPath.listFiles()?.forEach { file ->
        if (file.isDirectory) file.deleteRecursively() else file.delete()
      }
    }
  }
}
