package vivek.example.kite

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.jms.annotation.EnableJms

@SpringBootApplication
@EnableJms
@ConfigurationPropertiesScan(basePackages = ["vivek.example.kite"])
class KiteAlertsApplication

fun main(args: Array<String>) {
  runApplication<KiteAlertsApplication>(*args)
}
