package vivek.example.kite.ams.repository

import vivek.example.kite.ams.model.Alert

/**
 * An interface for fetching alert definitions. This abstracts the data source (mock, RocksDB,
 * primary database) from the caching and matching logic.
 */
fun interface AlertRepository {
  fun findActiveAlertsForSymbols(symbols: List<String>): List<Alert>
}
