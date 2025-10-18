package vivek.example.kite.ams.service

import jakarta.persistence.EntityManager
import jakarta.persistence.EntityNotFoundException
import java.math.BigDecimal
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertChangeEvent
import vivek.example.kite.ams.model.AlertOutboxEvent
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.repository.AlertOutboxRepository
import vivek.example.kite.ams.repository.PostgresAlertRepository
import vivek.example.kite.common.config.CommonProperties

@Service
class AlertManagementService(
    private val commonProperties: CommonProperties,
    private val alertRepository: PostgresAlertRepository,
    private val outboxRepository: AlertOutboxRepository,
    private val entityManager: EntityManager
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  @Transactional
  fun createAlert(request: AlertRequest): Alert {

    val alertId =
        "${request.stockSymbol}_${
                request.conditionType.toString()
                    .lowercase()
            }_${request.priceThreshold}"

    validateAlertRequest(alertId, request)

    try {
      val alertToSave =
          Alert(
              alertId = alertId,
              stockSymbol = request.stockSymbol,
              userId = request.userId,
              priceThreshold = request.priceThreshold,
              conditionType = request.conditionType)

      val savedAlert = saveAndRefresh(alertToSave)
      val event = AlertChangeEvent(alert = savedAlert, eventType = "CREATE")
      outboxRepository.save(AlertOutboxEvent(payload = event))
      logger.info(
          "Alert created successfully for symbol: {} with key : {}", request.stockSymbol, alertId)
      return savedAlert
    } catch (e: Exception) {
      logger.error(
          "Failed to create alert for symbol: {} with key : {}", request.stockSymbol, alertId, e)
      throw e
    }
  }

  /**
   * Deactivates an alert and creates an outbox event to propagate this change. This is the
   * mechanism that makes alerts "one-time".
   */
  @Transactional
  fun deactivateAlert(id: UUID): Alert {
    try {
      val alert =
          alertRepository.findById(id).orElseThrow {
            EntityNotFoundException("Alert not found with id: $id")
          }
      if (!alert.isActive) return alert // Already inactive

      val deactivatedAlert = alert.copy(isActive = false)

      val savedAlert = saveAndRefresh(deactivatedAlert)
      val event = AlertChangeEvent(alert = savedAlert, eventType = "DELETE")
      outboxRepository.save(AlertOutboxEvent(payload = event))
      return savedAlert
    } catch (e: Exception) {
      logger.error("Failed to deactivate alert with id: $id", e)
      throw e
    }
  }

  fun findAlertById(id: UUID): Alert? {
    return alertRepository.findById(id).orElse(null)
  }

  @Transactional
  fun saveAndRefresh(alert: Alert): Alert {
    val saved = alertRepository.save(alert)
    entityManager.flush()
    entityManager.refresh(saved)
    return saved
  }

  @Transactional
  fun validateAlertRequest(alertId: String, request: AlertRequest) {
    if (!commonProperties.stocks.contains(request.stockSymbol)) {
      throw InvalidStockException(
          "Invalid stock symbol: ${request.stockSymbol}, Allowed stocks are ${commonProperties.stocks}")
    }

    if (request.priceThreshold <= BigDecimal.ZERO) {
      throw InvalidPriceException(
          "Invalid price threshold: ${request.priceThreshold}, Price threshold must be greater than 0")
    }

    // Check if an active alert with the same business key already exists for the user
    val existingAlert =
        alertRepository.findByAlertIdAndUserIdAndIsActiveTrue(alertId, request.userId)

    if (existingAlert != null) {
      throw AlertAlreadyExistsException(
          "An alert already exists for user ${request.userId} symbol ${request.stockSymbol} with condition ${request.conditionType} and price ${request.priceThreshold}")
    }
  }
}

class InvalidStockException(message: String) : RuntimeException(message)

class InvalidPriceException(message: String) : RuntimeException(message)

class AlertAlreadyExistsException(message: String) : RuntimeException(message)
