package vivek.example.kite.ams.repository

import java.time.Instant
import java.util.UUID
import org.springframework.data.domain.Page
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.Alert

@Repository
interface PostgresAlertRepository : JpaRepository<Alert, UUID> {
  fun findByAlertIdAndUserIdAndIsActiveTrue(alertId: String, userId: String): Alert?

  fun findByIsActiveTrueAndStockSymbolIn(symbols: List<String>): List<Alert>

  /**
   * Efficiently fetches a page of active alerts for a given list of symbols created after a
   * specific time. This is the core of the cursor-based pagination for the reconciliation service.
   */
  fun findByIsActiveTrueAndStockSymbolInAndCreatedAtAfterOrderByCreatedAt(
      symbols: List<String>,
      createdAt: Instant,
      pageable: Pageable
  ): Page<Alert>
}
