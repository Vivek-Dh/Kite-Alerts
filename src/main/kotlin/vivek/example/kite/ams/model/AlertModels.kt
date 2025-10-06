package vivek.example.kite.ams.model

import java.math.BigDecimal

enum class ConditionType {
  GT,
  GTE,
  LT,
  LTE,
  EQ
}

data class Alert(
    val alertId: String,
    val stockSymbol: String,
    val userId: String,
    val priceThreshold: BigDecimal,
    val conditionType: ConditionType,
    val isOneTime: Boolean,
    val updatedAt: Long
)

// Minimal info stored in the L1 in-memory cache lists
data class AlertDetail(
    val alertId: String,
    // We store conditionType here in case we consolidate maps later and need to post-filter
    val conditionType: ConditionType
)
