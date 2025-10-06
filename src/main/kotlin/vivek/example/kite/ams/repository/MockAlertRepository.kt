package vivek.example.kite.ams.repository

import java.math.BigDecimal
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.ConditionType

/**
 * An in-memory mock repository for user alerts. In a real application, this would connect to a
 * persistent database (L3).
 *
 * This implementation generates a consistent set of mock alerts for any given list of symbols.
 */
class MockAlertRepository : AlertRepository {

  override fun findActiveAlertsForSymbols(symbols: List<String>): List<Alert> {
    // Generate a consistent set of mock alerts for the given symbols
    return symbols.flatMap { symbol ->
      val basePrice = getBasePriceForSymbol(symbol)
      createMockAlertsForPrice(symbol, basePrice)
    }
  }

  private fun getBasePriceForSymbol(symbol: String): Double {
    return when (symbol) {
      "RELIANCE" -> 2800.0
      "HDFCBANK" -> 1500.0
      "ICICIBANK" -> 950.0
      "INFY" -> 1400.0
      "TCS" -> 3400.0
      "SBIN" -> 580.0
      "KOTAKBANK" -> 1800.0
      "WIPRO" -> 450.0 // Add a stable base price for WIPRO for predictable tests
      "ASIANPAINT" -> 1200.0
      "NESTLEIND" -> 1500.0
      else -> (kotlin.math.abs(symbol.hashCode() % 200) * 10.0) + 100.0
    }
  }

  private fun createMockAlertsForPrice(symbol: String, basePrice: Double): List<Alert> {
    val priceUp1 = (basePrice * 1.01).toBigDecimal().setScale(2, BigDecimal.ROUND_HALF_UP)
    val priceUp2 = (basePrice * 1.02).toBigDecimal().setScale(2, BigDecimal.ROUND_HALF_UP)
    val priceDown1 = (basePrice * 0.99).toBigDecimal().setScale(2, BigDecimal.ROUND_HALF_UP)
    val priceDown2 = (basePrice * 0.98).toBigDecimal().setScale(2, BigDecimal.ROUND_HALF_UP)

    return listOf(
        Alert(
            "${symbol}_gte_${priceUp1}",
            symbol,
            "user1",
            priceUp1,
            ConditionType.GTE,
            false,
            System.currentTimeMillis()),
        Alert(
            "${symbol}_gt_${priceUp2}",
            symbol,
            "user2",
            priceUp2,
            ConditionType.GT,
            true,
            System.currentTimeMillis()),
        Alert(
            "${symbol}_lte_${priceDown1}",
            symbol,
            "user3",
            priceDown1,
            ConditionType.LTE,
            false,
            System.currentTimeMillis()),
        Alert(
            "${symbol}_lt_${priceDown2}",
            symbol,
            "user4",
            priceDown2,
            ConditionType.LT,
            false,
            System.currentTimeMillis()),
        Alert(
            "${symbol}_eq_${basePrice.toBigDecimal()}",
            symbol,
            "user5",
            basePrice.toBigDecimal(),
            ConditionType.EQ,
            false,
            System.currentTimeMillis()))
  }
}
