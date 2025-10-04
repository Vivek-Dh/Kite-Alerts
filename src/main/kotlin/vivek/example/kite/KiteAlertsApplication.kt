package vivek.example.kite

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.jms.annotation.EnableJms

@SpringBootApplication
@EnableJms
@ConfigurationPropertiesScan(
    "vivek.example.kite.config",
    "vivek.example.kite.tickprocessor.config",
    "vivek.example.kite.ams.config" // Add ams config to scan path
    )
class KiteAlertsApplication

fun main(args: Array<String>) {
  runApplication<KiteAlertsApplication>(*args)
}
