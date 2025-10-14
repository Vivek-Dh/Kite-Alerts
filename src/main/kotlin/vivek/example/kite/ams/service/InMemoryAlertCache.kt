package vivek.example.kite.ams.service

import java.math.BigDecimal
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.text.get
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
    private val alertRepository: AlertRepository,
    private val assignedSymbols: List<String>
) {
  // Add a simple map to fetch full alert details by ID quickly
  private val alertsById = ConcurrentHashMap<UUID, Alert>()
  private val cache = mutableMapOf<String, SymbolAlertsContainer>()

  fun initializeCache() {
    if (assignedSymbols.isEmpty()) return
    val alerts = alertRepository.findActiveAlertsForSymbols(assignedSymbols)
    alerts.forEach { alert ->
      addAlert(alert)
      alertsById[alert.id] = alert
    }
  }

  fun getAlertsForSymbol(symbol: String): SymbolAlertsContainer? = cache[symbol]

  fun getAlertDetails(id: UUID): Alert? = alertsById[id]

  fun getActiveAlerts(): Set<Alert> {
    return alertsById.values.toSet()
  }

  fun addAlert(alert: Alert) {
    val symbolCache = cache.computeIfAbsent(alert.stockSymbol) { SymbolAlertsContainer() }
    val alertDetail = AlertDetail(alert.id, alert.alertId, alert.conditionType)

    when (alert.conditionType) {
      ConditionType.GT,
      ConditionType.GTE ->
          symbolCache.upperBoundAlerts
              .computeIfAbsent(alert.priceThreshold) { mutableListOf() }
              .add(alertDetail)
      ConditionType.LT,
      ConditionType.LTE ->
          symbolCache.lowerBoundAlerts
              .computeIfAbsent(alert.priceThreshold) { mutableListOf() }
              .add(alertDetail)
      ConditionType.EQ ->
          symbolCache.equalsAlerts
              .computeIfAbsent(alert.priceThreshold) { mutableListOf() }
              .add(alertDetail)
    }
    alertsById[alert.id] = alert
  }

  fun removeAlert(id: UUID): Boolean {
    val alert = alertsById[id] ?: return false
    val symbolCache = cache[alert.stockSymbol] ?: return false

    val alertList =
        when (alert.conditionType) {
          ConditionType.GT,
          ConditionType.GTE -> symbolCache.upperBoundAlerts[alert.priceThreshold]
          ConditionType.LT,
          ConditionType.LTE -> symbolCache.lowerBoundAlerts[alert.priceThreshold]
          ConditionType.EQ -> symbolCache.equalsAlerts[alert.priceThreshold]
        }

    alertList?.removeIf { it.id == id }
    alertsById.remove(id)
    return true
  }
}
