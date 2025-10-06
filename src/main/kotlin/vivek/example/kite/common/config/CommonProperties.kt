package vivek.example.kite.common.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "common")
data class CommonProperties(val aggregatedUpdatesTopic: String, val stocks: List<String>)
