package vivek.example.kite.ams.model

import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

enum class ConditionType {
  GT,
  GTE,
  LT,
  LTE,
  EQ
}

data class Alert(
    val id: UUID,
    val alertId: String,
    val stockSymbol: String,
    val userId: String,
    val priceThreshold: BigDecimal,
    val conditionType: ConditionType,
    val isOneTime: Boolean,
    val updatedAt: Long,
    val updatedAtUtc: LocalDateTime =
        Instant.ofEpochMilli(updatedAt).atZone(ZoneOffset.UTC).toLocalDateTime()
)

data class AlertRequest(
    val stockSymbol: String,
    val userId: String,
    val priceThreshold: BigDecimal,
    val conditionType: ConditionType
)

// Minimal info stored in the L1 in-memory cache lists
data class AlertDetail(
    val id: UUID,
    val alertId: String,
    // We store conditionType here in case we consolidate maps later and need to post-filter
    val conditionType: ConditionType
)

// New data class for the event published when an alert is triggered
data class TriggeredAlertEvent(
    val eventId: String,
    val alert: Alert, // Include the full original alert for context
    val triggeredAt: Long,
    val triggeredAtUtc: LocalDateTime =
        Instant.ofEpochMilli(triggeredAt).atZone(ZoneOffset.UTC).toLocalDateTime(),
    val triggeredPrice: BigDecimal, // The price (High or Low) that caused the trigger
    val window: AggregatedLHWindow,
    val message: String
)
