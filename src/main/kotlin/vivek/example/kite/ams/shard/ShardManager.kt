package vivek.example.kite.ams.shard

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.jms.MessageListener
import java.time.Clock
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jms.config.DefaultJmsListenerContainerFactory
import org.springframework.jms.config.JmsListenerEndpointRegistry
import org.springframework.jms.config.SimpleJmsListenerEndpoint
import org.springframework.jms.support.converter.MessageConverter
import org.springframework.stereotype.Component
import vivek.example.kite.ams.config.AmsProperties
import vivek.example.kite.ams.model.Alert
import vivek.example.kite.ams.model.AlertRequest
import vivek.example.kite.ams.repository.AlertRepository
import vivek.example.kite.ams.service.AlertMatchingService
import vivek.example.kite.ams.service.InMemoryAlertCache
import vivek.example.kite.common.config.CommonProperties
import vivek.example.kite.common.service.SymbolService
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

@Component
class ShardManager(
    private val clock: Clock,
    private val commonProperties: CommonProperties,
    private val amsProperties: AmsProperties,
    private val alertMatchingService: AlertMatchingService,
    private val shardingStrategy: ShardingStrategy,
    private val symbolService: SymbolService,
    private val registry: JmsListenerEndpointRegistry,
    private val messageConverter: MessageConverter,
    @Qualifier("amsListenerContainerFactory")
    private val amsListenerContainerFactory: DefaultJmsListenerContainerFactory,
    private val alertRepository: AlertRepository
) {
  private val logger = LoggerFactory.getLogger(javaClass)
  private val shards = mutableMapOf<String, Shard>()
  private lateinit var assignments: Map<String, List<String>>

  @PostConstruct
  fun initializeShards() {
    logger.info("Dynamically assigning symbols to shards using consistent hashing...")

    val allSymbols = symbolService.getAllSymbols()
    val availableShards = amsProperties.shards.keys.toList()

    logger.info("All symbols: $allSymbols")
    logger.info("Available shards: $availableShards")

    if (availableShards.isEmpty()) {
      logger.warn("No AMS shards configured. Alert matching will be disabled.")
      return
    }

    assignments = shardingStrategy.assign(allSymbols, availableShards)
    logger.info("Sharding assignments complete: $assignments")

    assignments.forEach { (shardName, assignedSymbols) ->
      val shardConfig = amsProperties.shards[shardName]!!
      logger.info("--> Creating shard: '$shardName' for symbols: $assignedSymbols")

      val inMemoryCache = InMemoryAlertCache(alertRepository, assignedSymbols)
      inMemoryCache.initializeCache()

      val shard = Shard(shardName, assignedSymbols, inMemoryCache)
      shards[shardName] = shard

      registerListenerForShard(shard, shardConfig.listenerConcurrency)
    }

    val unassignedShards = availableShards.filterNot { assignments.containsKey(it) }
    unassignedShards.forEach { shardName ->
      logger.warn(
          "--> Shard '$shardName' was configured but received no symbol assignments. It will be idle.")
    }
  }

  // New public method to allow tests to access shard information
  fun getShardForSymbol(symbol: String): Shard? {
    if (!this::assignments.isInitialized) return null
    val shardName = assignments.entries.find { symbol in it.value }?.key ?: return null
    return shards[shardName]
  }

  fun createAlert(alertRequest: AlertRequest): Alert {
    val alertId =
        "${alertRequest.stockSymbol}_${alertRequest.conditionType.toString().lowercase()}_${alertRequest.priceThreshold}"
    val cache = getShardForSymbol(alertRequest.stockSymbol)?.cache!!
    if (cache.getActiveAlerts().find { alert ->
      alert.userId == alertRequest.userId && alert.alertId == alertId
    } != null) {
      throw IllegalArgumentException(
          "Alert already exists for user ${alertRequest.userId} and condition--price ${alertRequest.conditionType}_${alertRequest.priceThreshold}")
    }
    val alert =
        Alert(
            UUID.randomUUID(),
            alertId,
            alertRequest.stockSymbol,
            alertRequest.userId,
            alertRequest.priceThreshold,
            alertRequest.conditionType,
            true,
            clock.instant().toEpochMilli())

    cache.addAlert(alert)
    logger.info("Alert created $alert")
    return cache.getAlertDetails(alert.id)!!
  }

  private fun registerListenerForShard(shard: Shard, concurrency: String) {
    val endpointId = "ams-shard-listener-${shard.name}"
    val endpoint = SimpleJmsListenerEndpoint()
    endpoint.id = endpointId
    endpoint.destination = commonProperties.aggregatedUpdatesTopic
    endpoint.selector = createSelectorForSymbols(shard.assignedSymbols)
    endpoint.concurrency = concurrency // Set shard-specific concurrency on the endpoint itself

    endpoint.messageListener = MessageListener { message ->
      try {
        val window = messageConverter.fromMessage(message) as AggregatedLHWindow
        alertMatchingService.processMessageForShard(window, shard)
      } catch (e: Exception) {
        logger.error("[{}] Error processing message for shard.", shard.name, e)
      }
    }

    // We MUST provide a unique subscription name for each shard's listener to ensure
    // they function as independent durable/shared consumers on the topic.
    // The endpoint's subscription name takes precedence over the factory's.
    endpoint.subscription = "ams-subscription-${shard.name}"

    // Register the endpoint using the pre-configured factory as a template.
    registry.registerListenerContainer(endpoint, amsListenerContainerFactory)
    logger.info(
        "Registered JMS listener for shard '{}' with ID '{}' and subscription name '{}'",
        shard.name,
        endpointId,
        endpoint.subscription)
  }

  private fun createSelectorForSymbols(symbols: List<String>): String {
    if (symbols.isEmpty()) {
      // This selector will never match any messages, effectively disabling the listener.
      return "1=0"
    }
    val symbolsString = symbols.joinToString("','", "'", "'")
    return "stockSymbol IN ($symbolsString)"
  }

  @PreDestroy
  fun shutdown() {
    logger.info("Shutting down all AMS shard listeners.")
    shards.keys.forEach { shardName ->
      val containerId = "ams-shard-listener-$shardName"
      val container = registry.getListenerContainer(containerId)
      container?.stop()
    }
    registry.destroy()
    shards.clear()
  }
}
