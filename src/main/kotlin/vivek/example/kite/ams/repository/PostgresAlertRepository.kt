package vivek.example.kite.ams.repository

import java.util.UUID
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.Alert

@Repository
interface PostgresAlertRepository : JpaRepository<Alert, UUID> {
  fun findByAlertIdAndUserId(alertId: String, userId: String): Alert?
}
