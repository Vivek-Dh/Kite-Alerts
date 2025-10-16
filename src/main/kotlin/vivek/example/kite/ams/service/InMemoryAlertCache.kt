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
    private val alertRepository: AlertRepository, // Now sourced from L2 (RocksDB)
    private val assignedSymbols: List<String>
) {
  // Add a simple map to fetch full alert details by ID quickly
  private val alertsById = ConcurrentHashMap<UUID, Alert>()
  private val cache = mutableMapOf<String, SymbolAlertsContainer>()

  fun initializeCache() {
    if (assignedSymbols.isEmpty()) return
    val alerts = alertRepository.findActiveAlertsForSymbols(assignedSymbols)
    alerts.forEach { alert -> addAlert(alert) }
  }

  fun getAlertsForSymbol(symbol: String): SymbolAlertsContainer? = cache[symbol]

  fun getAlertDetails(id: UUID): Alert? = alertsById[id]

  fun getActiveAlerts(): Set<Alert> {
    return alertsById.values.filter { it.isActive }.toSet()
  }

  fun addAlert(alert: Alert) {
    alertsById[alert.id] = alert
    if (!alert.isActive) return // Don't add inactive alerts to matching maps

    val symbolCache = cache.computeIfAbsent(alert.stockSymbol) { SymbolAlertsContainer() }
    val alertDetail =
        AlertDetail(
            alert.id, alert.alertId, alert.conditionType, alert.priceThreshold, alert.isActive)

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

  /**
   * Intelligently updates an alert in the cache. It handles creation, price/condition changes, and
   * deactivation by removing the old entry from the price maps and adding the new one if active.
   */
  fun updateAlert(updatedAlert: Alert) {
    val oldAlert = alertsById[updatedAlert.id]

    // If the alert previously existed, remove its old entry from the price maps.
    if (oldAlert != null && oldAlert.isActive) {
      val symbolCache = cache[oldAlert.stockSymbol]
      if (symbolCache != null) {
        val oldList =
            when (oldAlert.conditionType) {
              ConditionType.GT,
              ConditionType.GTE -> symbolCache.upperBoundAlerts[oldAlert.priceThreshold]
              ConditionType.LT,
              ConditionType.LTE -> symbolCache.lowerBoundAlerts[oldAlert.priceThreshold]
              ConditionType.EQ -> symbolCache.equalsAlerts[oldAlert.priceThreshold]
            }
        oldList?.removeIf { it.id == oldAlert.id }
      }
    }

    // Add the alert (active or inactive) to the main ID map. This ensures we can always look it up.
    alertsById[updatedAlert.id] = updatedAlert

    // If the updated alert is active, add it back to the correct price map.
    if (updatedAlert.isActive) {
      val symbolCache = cache.computeIfAbsent(updatedAlert.stockSymbol) { SymbolAlertsContainer() }
      val alertDetail =
          AlertDetail(
              updatedAlert.id,
              updatedAlert.alertId,
              updatedAlert.conditionType,
              updatedAlert.priceThreshold,
              updatedAlert.isActive)
      val newList =
          when (updatedAlert.conditionType) {
            ConditionType.GT,
            ConditionType.GTE ->
                symbolCache.upperBoundAlerts.computeIfAbsent(updatedAlert.priceThreshold) {
                  mutableListOf()
                }
            ConditionType.LT,
            ConditionType.LTE ->
                symbolCache.lowerBoundAlerts.computeIfAbsent(updatedAlert.priceThreshold) {
                  mutableListOf()
                }
            ConditionType.EQ ->
                symbolCache.equalsAlerts.computeIfAbsent(updatedAlert.priceThreshold) {
                  mutableListOf()
                }
          }
      // Avoid adding duplicates
      if (newList.none { it.id == alertDetail.id }) {
        newList.add(alertDetail)
      }
    }
  }
}
