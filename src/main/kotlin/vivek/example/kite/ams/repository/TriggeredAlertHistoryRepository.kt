package vivek.example.kite.ams.repository

import java.util.UUID
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.TriggeredAlertHistory

@Repository
interface TriggeredAlertHistoryRepository : JpaRepository<TriggeredAlertHistory, UUID> {
  fun findByUserId(userId: String): List<TriggeredAlertHistory>
}
