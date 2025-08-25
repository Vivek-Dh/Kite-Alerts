package vivek.example.kite

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication class KiteAlertsApplication

fun main(args: Array<String>) {
  runApplication<KiteAlertsApplication>(*args)
}
