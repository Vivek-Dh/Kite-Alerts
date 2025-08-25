package vivek.example.kite.tickprocessor.model

import java.math.BigDecimal

data class TickData(
    val id: String,
    val symbol: String,
    val price: BigDecimal,
    val timestamp: Long,
    val volume: Long
)

data class AggregatedLHWindow(
    val symbol: String,
    val windowStartTime: Long,
    val windowEndTime: Long,
    val low: BigDecimal,
    val high: BigDecimal,
    val ticksCount: Int
)
