package vivek.example.kite.ams.repository

import java.math.BigDecimal
import java.util.UUID
import kotlin.random.Random
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.common.config.CommonProperties

/**
 * An in-memory mock repository for user alerts. This class is now primarily used by the
 * DataInitializer to generate a consistent set of mock data to be inserted into the L3 database on
 * startup.
 */
@Repository
class MockAlertRepository(private val commonProperties: CommonProperties) : AlertRepository {

  companion object TestAlertIds {
    val RELIANCE_GTE_2828: UUID = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val RELIANCE_GT_2856: UUID = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val RELIANCE_LTE_2772: UUID = UUID.fromString("00000000-0000-0000-0000-000000000003")
    val RELIANCE_LT_2744: UUID = UUID.fromString("00000000-0000-0000-0000-000000000004")
    val RELIANCE_EQ_2800: UUID = UUID.fromString("00000000-0000-0000-0000-000000000005")
  }

  override fun findActiveAlertsForSymbols(symbols: List<String>): List<Alert> {
    // Generate a consistent set of mock alerts for the given symbols
    return symbols.flatMap { symbol ->
      val basePrice = getBasePriceForSymbol(symbol)
      createMockAlertsForPrice(symbol, basePrice)
    }
  }

  private fun getBasePriceForSymbol(symbol: String): BigDecimal {
    return commonProperties.initialPrices[symbol]
        ?: (BigDecimal.valueOf(500) *
            BigDecimal.valueOf(Random.nextDouble(0.7, 1.3)).setScale(2, BigDecimal.ROUND_HALF_UP))
  }

  private fun createMockAlertsForPrice(symbol: String, basePrice: BigDecimal): List<Alert> {
    val priceUp1 = (basePrice * BigDecimal.valueOf(1.01)).setScale(2, BigDecimal.ROUND_HALF_UP)
    val priceUp2 = (basePrice * BigDecimal.valueOf(1.02)).setScale(2, BigDecimal.ROUND_HALF_UP)
    val priceDown1 = (basePrice * BigDecimal.valueOf(0.99)).setScale(2, BigDecimal.ROUND_HALF_UP)
    val priceDown2 = (basePrice * BigDecimal.valueOf(0.98)).setScale(2, BigDecimal.ROUND_HALF_UP)

    // Timestamps are no longer needed here; the database will set them.
    return listOf(
        Alert(
            id = if (symbol == "RELIANCE") RELIANCE_GTE_2828 else UUID.randomUUID(),
            alertId = "${symbol}_gte_${priceUp1}",
            stockSymbol = symbol,
            userId = "user_common",
            priceThreshold = priceUp1,
            conditionType = ConditionType.GTE),
        Alert(
            id = if (symbol == "RELIANCE") RELIANCE_GT_2856 else UUID.randomUUID(),
            alertId = "${symbol}_gt_${priceUp2}",
            stockSymbol = symbol,
            userId = "user_common",
            priceThreshold = priceUp2,
            conditionType = ConditionType.GT,
            isActive = true),
        Alert(
            id = if (symbol == "RELIANCE") RELIANCE_LTE_2772 else UUID.randomUUID(),
            alertId = "${symbol}_lte_${priceDown1}",
            stockSymbol = symbol,
            userId = "user_1",
            priceThreshold = priceDown1,
            conditionType = ConditionType.LTE),
        Alert(
            id = if (symbol == "RELIANCE") RELIANCE_LT_2744 else UUID.randomUUID(),
            alertId = "${symbol}_lt_${priceDown2}",
            stockSymbol = symbol,
            userId = "user_2",
            priceThreshold = priceDown2,
            conditionType = ConditionType.LT),
        Alert(
            id = if (symbol == "RELIANCE") RELIANCE_EQ_2800 else UUID.randomUUID(),
            alertId = "${symbol}_eq_${basePrice}",
            stockSymbol = symbol,
            userId = "user_1",
            priceThreshold = basePrice,
            conditionType = ConditionType.EQ))
  }
}
