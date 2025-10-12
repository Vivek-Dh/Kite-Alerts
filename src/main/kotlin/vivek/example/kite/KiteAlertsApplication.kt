package vivek.example.kite

import java.time.Clock
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.jms.annotation.EnableJms

@SpringBootApplication
@EnableJms
@ConfigurationPropertiesScan(basePackages = ["vivek.example.kite"])
class KiteAlertsApplication {
  @Bean
  fun clock(): Clock {
    return Clock.systemUTC()
  }
}

fun main(args: Array<String>) {
  runApplication<KiteAlertsApplication>(*args)
}
