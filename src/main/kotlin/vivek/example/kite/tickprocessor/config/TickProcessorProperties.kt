package vivek.example.kite.tickprocessor.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "tick-processor")
data class TickProcessorProperties(
    val jms: JmsProperties,
    val mockProducer: MockProducerProperties,
    val windowAggregator: WindowAggregatorProperties,
    val priceStream: PriceStreamProperties
)

data class JmsProperties(val topics: Topics)

data class Topics(
    val rawTicksHighActivity: String,
    val rawTicksMediumActivity: String,
    val rawTicksLowActivity: String,
    val aggregatedUpdates: String
)

data class MockProducerProperties(
    val enabled: Boolean = true,
    val stockCategories: Map<String, List<String>>,
    var tickVolatility: Double = 0.005,
    var driftUpdateTicks: Int = 1000,
    var driftBiasRange: Double = 0.1,
    val frequencyMillis: Map<String, Long>
)

data class WindowAggregatorProperties(
    val enabled: Boolean = true,
    val timerCheckIntervalMillis: Long = 50,
    val windowDurationMillis: Map<String, Long>,
    val listenerConcurrency: Map<String, String>
)

data class PriceStreamProperties(val listenerConcurrency: String)
