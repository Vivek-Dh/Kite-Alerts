package vivek.example.kite.ams.service

import java.math.BigDecimal
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import vivek.example.kite.ams.config.AmsProperties
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.ams.shard.Shard
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

@Service
class AlertMatchingService(private val amsProperties: AmsProperties) {
  private val logger = LoggerFactory.getLogger(javaClass)

  fun processMessageForShard(window: AggregatedLHWindow, shard: Shard) {
    val symbol = window.symbol
    val threadName = Thread.currentThread().name
    logger.info(
        "Shard : [{}] -> Processing message for '{}' on thread '{}'",
        shard.name,
        symbol,
        threadName)

    if (symbol !in shard.assignedSymbols) {
      logger.warn(
          "Shard : [{}] Received message for unassigned symbol '{}'. Ignoring.", shard.name, symbol)
      return
    }

    // --- SIMULATE WORK to make concurrency visible in tests ---
    if (amsProperties.simulateWorkDelayMs > 0) {
      logger.info(
          "Shard : [{}] Simulating work delay of {} ms",
          shard.name,
          amsProperties.simulateWorkDelayMs)
      Thread.sleep(amsProperties.simulateWorkDelayMs)
    }
    // ---------------------------------------------------------

    val symbolAlerts = shard.cache.getAlertsForSymbol(symbol)
    if (symbolAlerts == null) {
      logger.debug("Shard : [{}] No alerts configured for symbol '{}'.", shard.name, symbol)
      // Log finish time even if no alerts are configured
      logger.info(
          "Shard : [{}] <- Finished processing for '{}' on thread '{}'",
          shard.name,
          symbol,
          threadName)
      return
    }

    val triggeredAlerts = mutableSetOf<String>()

    triggeredAlerts.addAll(matchUpperBoundAlerts(window.high, symbolAlerts))
    triggeredAlerts.addAll(matchLowerBoundAlerts(window.low, symbolAlerts))
    triggeredAlerts.addAll(matchEqualsAlerts(window.low, window.high, symbolAlerts))

    if (triggeredAlerts.isNotEmpty()) {
      logger.info(
          "Shard : [{}] TRIGGERED ALERTS for {}: {}",
          shard.name,
          symbol,
          triggeredAlerts.joinToString())
    }

    logger.info(
        "Shard : [{}] <- Finished processing for '{}' on thread '{}'",
        shard.name,
        symbol,
        threadName)
  }

  private fun matchUpperBoundAlerts(
      highPrice: BigDecimal,
      alerts: SymbolAlertsContainer
  ): Set<String> {
    val triggered = mutableSetOf<String>()
    // GTE alerts: priceThreshold <= highPrice
    alerts.upperBoundAlerts.headMap(highPrice, true).values.forEach { alertDetails ->
      alertDetails.forEach { detail ->
        if (detail.conditionType == ConditionType.GTE) {
          triggered.add(detail.alertId)
        }
      }
    }
    // GT alerts: priceThreshold < highPrice
    alerts.upperBoundAlerts.headMap(highPrice, false).values.forEach { alertDetails ->
      alertDetails.forEach { detail ->
        if (detail.conditionType == ConditionType.GT) {
          triggered.add(detail.alertId)
        }
      }
    }
    return triggered
  }

  private fun matchLowerBoundAlerts(
      lowPrice: BigDecimal,
      alerts: SymbolAlertsContainer
  ): Set<String> {
    val triggered = mutableSetOf<String>()
    // LTE alerts: priceThreshold >= lowPrice
    alerts.lowerBoundAlerts.tailMap(lowPrice, true).values.forEach { alertDetails ->
      alertDetails.forEach { detail ->
        if (detail.conditionType == ConditionType.LTE) {
          triggered.add(detail.alertId)
        }
      }
    }
    // LT alerts: priceThreshold > lowPrice
    alerts.lowerBoundAlerts.tailMap(lowPrice, false).values.forEach { alertDetails ->
      alertDetails.forEach { detail ->
        if (detail.conditionType == ConditionType.LT) {
          triggered.add(detail.alertId)
        }
      }
    }
    return triggered
  }

  private fun matchEqualsAlerts(
      lowPrice: BigDecimal,
      highPrice: BigDecimal,
      alerts: SymbolAlertsContainer
  ): Set<String> {
    return alerts.equalsAlerts
        .subMap(lowPrice, true, highPrice, true)
        .values
        .flatMap { it.map { detail -> detail.alertId } }
        .toSet()
  }
}
