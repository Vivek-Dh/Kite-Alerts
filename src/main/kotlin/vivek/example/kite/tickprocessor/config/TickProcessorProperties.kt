package vivek.example.kite.tickprocessor.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "tick-processor")
class TickProcessorProperties {
  lateinit var jms: JmsProperties
  lateinit var mockProducer: MockProducerProperties
  lateinit var windowAggregator: WindowAggregatorProperties
  lateinit var priceStream: PriceStreamProperties

  class JmsProperties {
    lateinit var topics: Topics

    class Topics {
      lateinit var rawTicksHighActivity: String
      lateinit var rawTicksMediumActivity: String
      lateinit var rawTicksLowActivity: String
      lateinit var aggregatedUpdates: String
    }
  }

  class MockProducerProperties {
    var enabled: Boolean = true
    lateinit var stockCategories: Map<String, List<String>>
    var tickVolatility: Double = 0.005
    var driftUpdateTicks: Int = 1000
    var driftBiasRange: Double = 0.1
    lateinit var frequencyMillis: Map<String, Long>
  }

  class WindowAggregatorProperties {
    var enabled: Boolean = true
    var timerCheckIntervalMillis: Long = 50
    lateinit var windowDurationMillis: Map<String, Long>
    lateinit var listenerConcurrency: Map<String, String>
  }

  class PriceStreamProperties {
    lateinit var listenerConcurrency: String
  }
}
