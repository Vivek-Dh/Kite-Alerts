package vivek.example.kite.common.config

import java.math.BigDecimal
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "common")
data class CommonProperties(
    val alertDefinitionUpdatesTopic: String,
    val aggregatedUpdatesTopic: String,
    val triggeredAlertsTopic: String,
    val stocks: List<String>,
    val initialPrices: Map<String, BigDecimal>
)
