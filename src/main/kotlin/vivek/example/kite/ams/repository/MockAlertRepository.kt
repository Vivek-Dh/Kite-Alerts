package vivek.example.kite.ams.repository

import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.util.UUID
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.ConditionType

/**
 * An in-memory mock repository for user alerts. In a real application, this would connect to a
 * persistent database (L3).
 *
 * This implementation generates a consistent set of mock alerts for any given list of symbols.
 */
@Primary
@Repository
class MockAlertRepository(private val clock: Clock) : AlertRepository {

  companion object TestAlertIds {
    val RELIANCE_GTE_2828 = UUID.randomUUID()
    val RELIANCE_GT_2856 = UUID.randomUUID()
    val RELIANCE_LTE_2772 = UUID.randomUUID()
    val RELIANCE_LT_2744 = UUID.randomUUID()
    val RELIANCE_EQ_2800 = UUID.randomUUID()
  }

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
            if (symbol.equals("RELIANCE")) RELIANCE_GTE_2828 else UUID.randomUUID(),
            "${symbol}_gte_${priceUp1}",
            symbol,
            "user_common",
            priceUp1,
            ConditionType.GTE,
            false,
            Instant.now(clock).toEpochMilli()),
        Alert(
            if (symbol.equals("RELIANCE")) RELIANCE_GT_2856 else UUID.randomUUID(),
            "${symbol}_gt_${priceUp2}",
            symbol,
            "user_common",
            priceUp2,
            ConditionType.GT,
            true,
            Instant.now(clock).toEpochMilli()),
        Alert(
            if (symbol.equals("RELIANCE")) RELIANCE_LTE_2772 else UUID.randomUUID(),
            "${symbol}_lte_${priceDown1}",
            symbol,
            "user_1",
            priceDown1,
            ConditionType.LTE,
            false,
            Instant.now(clock).toEpochMilli()),
        Alert(
            if (symbol.equals("RELIANCE")) RELIANCE_LT_2744 else UUID.randomUUID(),
            "${symbol}_lt_${priceDown2}",
            symbol,
            "user_2",
            priceDown2,
            ConditionType.LT,
            false,
            Instant.now(clock).toEpochMilli()),
        Alert(
            if (symbol.equals("RELIANCE")) RELIANCE_EQ_2800 else UUID.randomUUID(),
            "${symbol}_eq_${basePrice.toBigDecimal()}",
            symbol,
            "user_1",
            basePrice.toBigDecimal(),
            ConditionType.EQ,
            false,
            Instant.now(clock).toEpochMilli()))
  }
}
