package vivek.example.kite

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling // Enables the @Scheduled annotation for our timer
class KiteAlertsApplication

fun main(args: Array<String>) {
  runApplication<KiteAlertsApplication>(*args)
}
