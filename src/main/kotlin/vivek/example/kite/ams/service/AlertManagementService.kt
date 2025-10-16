package vivek.example.kite.ams.service

import jakarta.persistence.EntityNotFoundException
import java.util.UUID
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertChangeEvent
import vivek.example.kite.ams.model.AlertOutboxEvent
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.repository.AlertOutboxRepository
import vivek.example.kite.ams.repository.PostgresAlertRepository

@Service
class AlertManagementService(
    private val alertRepository: PostgresAlertRepository,
    private val outboxRepository: AlertOutboxRepository,
) {

  @Transactional
  fun createOrUpdateAlert(request: AlertRequest): Alert {
    val alertId =
        "${request.stockSymbol}_${request.conditionType.toString().lowercase()}_${request.priceThreshold}"

    val existingAlert = alertRepository.findByAlertIdAndUserId(alertId, request.userId)

    // Timestamps are no longer set by the application.
    // The database's DEFAULT and ON UPDATE trigger will handle them.
    val alertToSave =
        existingAlert?.copy(
            priceThreshold = request.priceThreshold,
            isActive = true // Re-activate if it was inactive
            )
            ?: Alert(
                alertId = alertId,
                stockSymbol = request.stockSymbol,
                userId = request.userId,
                priceThreshold = request.priceThreshold,
                conditionType = request.conditionType)

    val savedAlert = alertRepository.save(alertToSave)
    val event =
        AlertChangeEvent(
            eventType = if (existingAlert == null) "CREATE" else "UPDATE", alert = savedAlert)
    // The DB will handle createdAt for the outbox event.
    outboxRepository.save(AlertOutboxEvent(payload = event))
    return savedAlert
  }

  @Transactional
  fun deactivateAlert(id: UUID): Alert {
    val alert =
        alertRepository.findById(id).orElseThrow {
          EntityNotFoundException("Alert not found with id: $id")
        }

    if (!alert.isActive) {
      return alert // Already inactive, do nothing
    }

    // The DB trigger will handle updating the 'updatedAt' timestamp.
    val deactivatedAlert = alert.copy(isActive = false)

    val savedAlert = alertRepository.save(deactivatedAlert)
    val event = AlertChangeEvent(eventType = "DELETE", alert = savedAlert)
    outboxRepository.save(AlertOutboxEvent(payload = event))
    return savedAlert
  }

  fun findAlertById(id: UUID): Alert? {
    return alertRepository.findById(id).orElse(null)
  }
}
