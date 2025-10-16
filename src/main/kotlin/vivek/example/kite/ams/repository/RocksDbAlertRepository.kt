package vivek.example.kite.ams.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.nio.charset.StandardCharsets
import java.util.Arrays
import org.slf4j.LoggerFactory
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.service.RocksDbService

class RocksDbAlertRepository(
    private val rocksDbService: RocksDbService,
    private val shardName: String,
    private val objectMapper: ObjectMapper
) : AlertRepository {

  private val logger = LoggerFactory.getLogger(javaClass)

  override fun findActiveAlertsForSymbols(symbols: List<String>): List<Alert> {
    val db = rocksDbService.getDb(shardName)
    val alerts = mutableListOf<Alert>()
    symbols.forEach { symbol ->
      val prefix = RocksDbService.getSymbolIndexPrefix(symbol)
      db.newIterator().use { iter ->
        // Seek to the first key that starts with our symbol prefix
        iter.seek(prefix)
        // Loop as long as the iterator is valid AND the key still has the same prefix
        while (iter.isValid && iter.key().startsWith(prefix)) {
          // The value of our index entry is the actual alert ID
          val alertIdBytes = iter.value()
          // Get the full alert data using the alert ID
          val alertBytes = db.get(alertIdBytes)
          if (alertBytes != null) {
            try {
              val alert = objectMapper.readValue<Alert>(alertBytes)
              if (alert.isActive) {
                alerts.add(alert)
              }
            } catch (e: Exception) {
              logger.error(
                  "Failed to deserialize alert for key ${String(alertIdBytes, StandardCharsets.UTF_8)}",
                  e)
            }
          }
          iter.next()
        }
      }
    }
    return alerts
  }

  /** Extension function to correctly check if a ByteArray starts with a given prefix ByteArray. */
  private fun ByteArray.startsWith(prefix: ByteArray): Boolean {
    if (prefix.size > this.size) return false
    return Arrays.equals(this, 0, prefix.size, prefix, 0, prefix.size)
  }
}
