package vivek.example.kite.ams.repository

import java.util.UUID
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.AlertOutboxEvent

@Repository
interface AlertOutboxRepository : JpaRepository<AlertOutboxEvent, UUID> {
  /**
   * Finds a batch of the oldest unprocessed outbox events. The `processedAt IS NULL` clause is the
   * key to finding unprocessed events. Spring Data JPA correctly translates this method name into
   * the appropriate query.
   */
  fun findTop100ByProcessedAtIsNullOrderByCreatedAt(): List<AlertOutboxEvent>
}
