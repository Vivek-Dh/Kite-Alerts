package vivek.example.kite.ams.service

import java.math.BigDecimal
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.text.get
import org.slf4j.LoggerFactory
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertDetail
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.ams.repository.AlertRepository

data class SymbolAlertsContainer(
    val upperBoundAlerts: ConcurrentSkipListMap<BigDecimal, MutableList<AlertDetail>> =
        ConcurrentSkipListMap(),
    val lowerBoundAlerts: ConcurrentSkipListMap<BigDecimal, MutableList<AlertDetail>> =
        ConcurrentSkipListMap(),
    val equalsAlerts: ConcurrentSkipListMap<BigDecimal, MutableList<AlertDetail>> =
        ConcurrentSkipListMap()
)

class InMemoryAlertCache(
    private val alertRepository: AlertRepository, // Now sourced from L2 (RocksDB)
    private val assignedSymbols: List<String>
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  // Add a simple map to fetch full alert details by ID quickly
  private val alertsById = ConcurrentHashMap<UUID, Alert>()
  // New index to enforce uniqueness on the business key (alertId + userId)
  private val alertsByBusinessKey = ConcurrentHashMap<String, UUID>()
  private val cache = mutableMapOf<String, SymbolAlertsContainer>()

  fun initializeCache() {
    if (assignedSymbols.isEmpty()) return
    val alerts = alertRepository.findActiveAlertsForSymbols(assignedSymbols)
    alerts.forEach { alert -> addAlert(alert) }
  }

  fun getAlertsForSymbol(symbol: String): SymbolAlertsContainer? = cache[symbol]

  fun getAlertDetails(id: UUID): Alert? = alertsById[id]

  fun getActiveAlerts(): Set<Alert> = alertsById.values.filter { it.isActive }.toSet()

  private fun addAlert(alert: Alert) {
    val businessKey = "${alert.alertId}::${alert.userId}"
    // Hard check for business key uniqueness before adding
    if (alertsByBusinessKey.containsKey(businessKey) &&
        alertsByBusinessKey[businessKey] != alert.id) {
      logger.warn(
          "Alert with business key {} already exists. Skipping duplicate alert.", businessKey)
      return // A different alert with the same logical key already exists.
    }

    alertsById[alert.id] = alert
    alertsByBusinessKey[businessKey] = alert.id

    if (!alert.isActive) return

    val symbolCache = cache.computeIfAbsent(alert.stockSymbol) { SymbolAlertsContainer() }
    val alertDetail =
        AlertDetail(
            alert.id, alert.alertId, alert.conditionType, alert.priceThreshold, alert.isActive)

    val list =
        when (alert.conditionType) {
          ConditionType.GT,
          ConditionType.GTE ->
              symbolCache.upperBoundAlerts.computeIfAbsent(alert.priceThreshold) { mutableListOf() }
          ConditionType.LT,
          ConditionType.LTE ->
              symbolCache.lowerBoundAlerts.computeIfAbsent(alert.priceThreshold) { mutableListOf() }
          ConditionType.EQ ->
              symbolCache.equalsAlerts.computeIfAbsent(alert.priceThreshold) { mutableListOf() }
        }
    if (list.none { it.id == alertDetail.id }) {
      list.add(alertDetail)
    }
  }

  fun removeAlert(id: UUID): Boolean {
    val alert = alertsById.remove(id) ?: return false
    val businessKey = "${alert.alertId}::${alert.userId}"
    alertsByBusinessKey.remove(businessKey)

    if (!alert.isActive) return true

    val symbolCache = cache[alert.stockSymbol] ?: return false

    val alertList =
        when (alert.conditionType) {
          ConditionType.GT,
          ConditionType.GTE -> symbolCache.upperBoundAlerts[alert.priceThreshold]
          ConditionType.LT,
          ConditionType.LTE -> symbolCache.lowerBoundAlerts[alert.priceThreshold]
          ConditionType.EQ -> symbolCache.equalsAlerts[alert.priceThreshold]
        }
    return alertList?.removeIf { it.id == id } ?: false
  }

  fun addOrUpdateAlert(alert: Alert) {
    val oldAlert = alertsById[alert.id]
    if (oldAlert != null) {
      removeAlert(oldAlert.id)
    }
    addAlert(alert)
  }
}
