package vivek.example.kite.ams.service

import java.math.BigDecimal
import java.util.concurrent.ConcurrentSkipListMap
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
  private val cache = mutableMapOf<String, SymbolAlertsContainer>()

  fun initializeCache() {
    if (assignedSymbols.isEmpty()) return
    val alerts = alertRepository.findActiveAlertsForSymbols(assignedSymbols)
    alerts.forEach { addAlert(it) }
  }

  fun getAlertsForSymbol(symbol: String): SymbolAlertsContainer? = cache[symbol]

  private fun addAlert(alert: Alert) {
    val symbolCache = cache.computeIfAbsent(alert.stockSymbol) { SymbolAlertsContainer() }
    val alertDetail = AlertDetail(alert.alertId, alert.conditionType)

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
  }
}
