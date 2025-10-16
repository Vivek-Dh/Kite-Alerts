package vivek.example.kite.ams.service

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.repository.MockAlertRepository
import vivek.example.kite.ams.repository.PostgresAlertRepository
import vivek.example.kite.common.service.SymbolService

@Component
class DataInitializer(
    private val alertRepository: PostgresAlertRepository,
    private val mockAlertRepository: MockAlertRepository,
    private val symbolService: SymbolService,
    private val alertManagementService: AlertManagementService // Inject the service
) {
  private val logger = LoggerFactory.getLogger(javaClass)

  @PostConstruct
  @Transactional
  fun init() {
    if (alertRepository.count() > 0) {
      logger.info("Database already contains alert data. Skipping mock data initialization.")
      return
    }

    logger.info("Database is empty. Populating with mock alerts...")
    val mockAlerts = mockAlertRepository.findActiveAlertsForSymbols(symbolService.getAllSymbols())

    mockAlerts.forEach { alert ->
      // Use the AlertManagementService to create alerts.
      // This ensures that an outbox event is created for each initial alert,
      // which will then be propagated to the L2 and L1 caches.
      val request =
          AlertRequest(
              stockSymbol = alert.stockSymbol,
              userId = alert.userId,
              priceThreshold = alert.priceThreshold,
              conditionType = alert.conditionType)
      alertManagementService.createOrUpdateAlert(request)
    }

    logger.info("Successfully populated database with {} mock alerts.", mockAlerts.size)
  }
}
