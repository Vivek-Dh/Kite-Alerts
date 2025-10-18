package vivek.example.kite.ams.service

import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jms.core.JmsTemplate
import org.springframework.stereotype.Service
import vivek.example.kite.ams.config.AmsProperties
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.shard.Shard
import vivek.example.kite.common.config.CommonProperties
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

@Service
class AlertMatchingService(
    private val clock: Clock,
    private val commonProperties: CommonProperties,
    private val amsProperties: AmsProperties,
    @Qualifier("jmsTopicTemplate") private val jmsTopicTemplate: JmsTemplate
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  fun processMessageForShard(window: AggregatedLHWindow, shard: Shard) {
    val symbol = window.symbol
    val threadName = Thread.currentThread().name
    logger.debug(
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

    val triggeredAlerts = mutableSetOf<UUID>()
    // A map to store which price triggered which alert
    val triggerDetails = mutableMapOf<UUID, BigDecimal>()

    matchUpperBoundAlerts(window.high, symbolAlerts).forEach {
      triggeredAlerts.add(it)
      triggerDetails[it] = window.high
    }
    matchLowerBoundAlerts(window.low, symbolAlerts).forEach {
      triggeredAlerts.add(it)
      triggerDetails[it] = window.low
    }
    matchEqualsAlerts(window.low, window.high, symbolAlerts).forEach {
      triggeredAlerts.add(it)
      triggerDetails[it] = window.high // Use high price as the "triggering" price for range match
    }

    if (triggeredAlerts.isNotEmpty()) {
      logger.info(
          "[{}] Matched Alerts for {}: {}, for window {}",
          shard.name,
          symbol,
          triggeredAlerts.joinToString(),
          window)

      // Instead of just logging, publish events to the new topic
      publishTriggeredAlerts(triggeredAlerts, triggerDetails, shard, window)
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
  ): Set<UUID> {
    val triggered = mutableSetOf<UUID>()
    alerts.upperBoundAlerts.headMap(highPrice, true).values.flatten().forEach { detail ->
      if (detail.conditionType == ConditionType.GTE) triggered.add(detail.id)
    }
    alerts.upperBoundAlerts.headMap(highPrice, false).values.flatten().forEach { detail ->
      if (detail.conditionType == ConditionType.GT) triggered.add(detail.id)
    }
    return triggered
  }

  private fun matchLowerBoundAlerts(
      lowPrice: BigDecimal,
      alerts: SymbolAlertsContainer
  ): Set<UUID> {
    val triggered = mutableSetOf<UUID>()
    alerts.lowerBoundAlerts.tailMap(lowPrice, true).values.flatten().forEach { detail ->
      if (detail.conditionType == ConditionType.LTE) triggered.add(detail.id)
    }
    alerts.lowerBoundAlerts.tailMap(lowPrice, false).values.flatten().forEach { detail ->
      if (detail.conditionType == ConditionType.LT) triggered.add(detail.id)
    }
    return triggered
  }

  private fun matchEqualsAlerts(
      lowPrice: BigDecimal,
      highPrice: BigDecimal,
      alerts: SymbolAlertsContainer
  ): Set<UUID> {
    return alerts.equalsAlerts
        .subMap(lowPrice, true, highPrice, true)
        .values
        .flatMap { it.map { detail -> detail.id } }
        .toSet()
  }

  private fun publishTriggeredAlerts(
      alertIds: Set<UUID>,
      triggerDetails: Map<UUID, BigDecimal>,
      shard: Shard,
      window: AggregatedLHWindow
  ) {
    alertIds.forEach { alertId ->
      val alert = shard.cache.getAlertDetails(alertId)
      if (alert == null) {
        logger.warn(
            "[{}] Could not find full alert details for triggered alertId: {}. Skipping publish.",
            shard.name,
            alertId)
        return@forEach
      }

      // CRITICAL FIX: DO NOT mutate the L1 cache directly.
      // The deactivation will flow through the L3 -> L2 -> L1 pipeline.
      // shard.cache.removeAlert(alertId) // This line is now removed.

      val triggeredPrice = triggerDetails[alertId] ?: BigDecimal.ZERO
      val event =
          TriggeredAlertEvent(
              eventId = UUID.randomUUID().toString(),
              alert = alert,
              triggeredAt = Instant.now(clock),
              triggeredPrice = triggeredPrice,
              window = window,
              message =
                  "Alert '${alert.alertId}' for user '${alert.userId}' triggered for symbol ${alert.stockSymbol}.")

      jmsTopicTemplate.convertAndSend(commonProperties.triggeredAlertsTopic, event) { message ->
        // Add properties for potential filtering by a notification service
        message.setStringProperty("userId", alert.userId)
        message.setStringProperty("stockSymbol", alert.stockSymbol)
        message
      }
      logger.info(
          "[{}] Published TriggeredAlertEvent for {} - alertId: {}, userId: {} and window: {}",
          shard.name,
          alert.id,
          alert.alertId,
          alert.userId,
          window)
    }
  }
}
