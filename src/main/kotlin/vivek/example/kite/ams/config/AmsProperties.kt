package vivek.example.kite.ams.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ams")
data class AmsProperties(val assignedSymbols: List<String>, val listenerConcurrency: String)
