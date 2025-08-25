package vivek.example.kite.tickprocessor.util

import java.math.BigDecimal
import java.math.RoundingMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PriceSimulatorTests {

  private val initialPrice = BigDecimal("100.00")
  private val minPrice = BigDecimal("70.00")
  private val maxPrice = BigDecimal("130.00")
  private val volatility = 0.005 // 0.5%

  private fun bd(value: String) = BigDecimal(value).setScale(2, RoundingMode.HALF_UP)

  @Test
  fun `should calculate a price increase with positive shock`() {
    val nextPrice =
        PriceSimulator.calculateNextPrice(
            currentPrice = initialPrice,
            minOverallPrice = minPrice,
            maxOverallPrice = maxPrice,
            driftBias = 0.0,
            tickVolatility = volatility,
            randomShock = 0.8 // Strong positive shock
            )
    // Expected change: 100 * (1 + (0.8 * 0.005)) = 100 * 1.004 = 100.40
    assertEquals(bd("100.40"), nextPrice)
  }

  @Test
  fun `should calculate a price decrease with negative shock`() {
    val nextPrice =
        PriceSimulator.calculateNextPrice(
            currentPrice = initialPrice,
            minOverallPrice = minPrice,
            maxOverallPrice = maxPrice,
            driftBias = 0.0,
            tickVolatility = volatility,
            randomShock = -0.5 // Medium negative shock
            )
    // Expected change: 100 * (1 + (-0.5 * 0.005)) = 100 * 0.9975 = 99.75
    assertEquals(bd("99.75"), nextPrice)
  }

  @Test
  fun `should clamp price at the maximum boundary`() {
    val priceNearMax = BigDecimal("129.90")
    val nextPrice =
        PriceSimulator.calculateNextPrice(
            currentPrice = priceNearMax,
            minOverallPrice = minPrice,
            maxOverallPrice = maxPrice,
            driftBias = 0.0,
            tickVolatility = volatility,
            randomShock = 1.0 // Max positive shock
            )
    // Potential price: 129.90 * (1 + 0.005) = 130.5495
    // Clamped price should be 130.00
    assertEquals(maxPrice, nextPrice)
  }

  @Test
  fun `should clamp price at the minimum boundary`() {
    val priceNearMin = BigDecimal("70.10")
    val nextPrice =
        PriceSimulator.calculateNextPrice(
            currentPrice = priceNearMin,
            minOverallPrice = minPrice,
            maxOverallPrice = maxPrice,
            driftBias = 0.0,
            tickVolatility = volatility,
            randomShock = -1.0 // Max negative shock
            )
    // Potential price: 70.10 * (1 - 0.005) = 69.7495
    // Clamped price should be 70.00
    assertEquals(minPrice, nextPrice)
  }

  @Test
  fun `should amplify upward move with positive drift bias`() {
    val nextPrice =
        PriceSimulator.calculateNextPrice(
            currentPrice = initialPrice,
            minOverallPrice = minPrice,
            maxOverallPrice = maxPrice,
            driftBias = 0.1, // Positive drift
            tickVolatility = volatility,
            randomShock = 0.8 // Strong positive shock
            )
    // Expected change: 100 * (1 + (0.8 * 0.005) + (0.1 * 0.005)) = 100 * (1 + 0.004 + 0.0005) =
    // 100.45
    assertEquals(bd("100.45"), nextPrice)
  }

  @Test
  fun `should dampen downward move with positive drift bias`() {
    val nextPrice =
        PriceSimulator.calculateNextPrice(
            currentPrice = initialPrice,
            minOverallPrice = minPrice,
            maxOverallPrice = maxPrice,
            driftBias = 0.1, // Positive drift
            tickVolatility = volatility,
            randomShock = -0.8 // Strong negative shock
            )
    // Expected change: 100 * (1 + (-0.8 * 0.005) + (0.1 * 0.005)) = 100 * (1 - 0.004 + 0.0005) =
    // 99.65
    assertEquals(bd("99.65"), nextPrice)
  }
}
