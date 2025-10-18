package vivek.example.kite.ams.service

import org.slf4j.LoggerFactory
import org.springframework.jms.annotation.JmsListener
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.model.TriggeredAlertEvent
import vivek.example.kite.ams.model.TriggeredAlertHistory
import vivek.example.kite.ams.repository.TriggeredAlertHistoryRepository

@Service
class TriggeredAlertHandlerService(
    private val alertManagementService: AlertManagementService,
    private val historyRepository: TriggeredAlertHistoryRepository
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  @JmsListener(
      destination = "\${common.triggeredAlertsTopic}",
      containerFactory = "amsListenerContainerFactory")
  @Transactional
  fun handleTriggeredAlert(event: TriggeredAlertEvent) {
    logger.debug("Handling triggered alert event for alertId: {}", event.alert.id)

    // 1. Persist the triggered event to the history table.
    val history =
        TriggeredAlertHistory(
            alertId = event.alert.id,
            userId = event.alert.userId,
            stockSymbol = event.alert.stockSymbol,
            triggeredPrice = event.triggeredPrice,
            triggeredAt = event.triggeredAt,
            alertPayload = event.alert)
    historyRepository.save(history)
    logger.debug("Persisted triggered alert history for alertId: {}", event.alert.id)

    // 2. Deactivate the original alert. This will create an outbox event.
    alertManagementService.deactivateAlert(event.alert.id)
    logger.debug("Deactivated alert for alertId: {}", event.alert.id)
  }
}
