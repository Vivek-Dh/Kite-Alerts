package vivek.example.kite.ams.repository

import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.util.UUID
import kotlin.random.Random
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Repository
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.ConditionType
import vivek.example.kite.common.config.CommonProperties

/**
 * An in-memory mock repository for user alerts. In a real application, this would connect to a
 * persistent database (L3).
 *
 * This implementation generates a consistent set of mock alerts for any given list of symbols.
 */
@Primary
@Repository
class MockAlertRepository(
    private val clock: Clock,
    private val commonProperties: CommonProperties
) : AlertRepository {

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
            "${symbol}_eq_${basePrice}",
            symbol,
            "user_1",
            basePrice,
            ConditionType.EQ,
            false,
            Instant.now(clock).toEpochMilli()))
  }
}
