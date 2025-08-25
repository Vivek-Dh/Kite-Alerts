package vivek.example.kite.tickprocessor.util

import java.math.BigDecimal
import java.math.RoundingMode

object PriceSimulator {

  /**
   * Calculates the next simulated stock price based on the current price and volatility parameters.
   * This is a pure function, making it easily testable.
   *
   * @param currentPrice The last known price of the stock.
   * @param minOverallPrice The absolute minimum price boundary for the stock.
   * @param maxOverallPrice The absolute maximum price boundary for the stock.
   * @param driftBias A small value (-1.0 to 1.0) indicating a longer-term trend.
   * @param tickVolatility The maximum percentage change for a single tick's random shock.
   * @param randomShock A random value between -1.0 and 1.0 representing the current tick's random
   *   movement.
   * @return The calculated next price, clamped within the boundaries.
   */
  fun calculateNextPrice(
      currentPrice: BigDecimal,
      minOverallPrice: BigDecimal,
      maxOverallPrice: BigDecimal,
      driftBias: Double,
      tickVolatility: Double,
      randomShock: Double
  ): BigDecimal {
    val randomShockEffect = randomShock * tickVolatility
    val driftEffect = driftBias * tickVolatility
    val changeMultiplier = BigDecimal(1.0 + randomShockEffect + driftEffect)

    val potentialNextPrice = currentPrice * changeMultiplier

    // Clamp the price within the defined boundaries and set the scale
    return potentialNextPrice
        .coerceIn(minOverallPrice, maxOverallPrice)
        .setScale(2, RoundingMode.HALF_UP)
  }
}
