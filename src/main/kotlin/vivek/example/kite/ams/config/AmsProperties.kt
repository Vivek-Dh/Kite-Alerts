package vivek.example.kite.ams.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ams")
data class AmsProperties( // A map where the key is the shard name (e.g., "shard-alpha")
    // and the value contains the configuration for that shard.
    val shards: Map<String, AmsShardConfig>,
    val consistentHashing: ConsistentHashingProperties,
    val simulateWorkDelayMs: Long = 0 // New property, defaults to 0
)

data class AmsShardConfig(val listenerConcurrency: String)

data class ConsistentHashingProperties(val virtualNodesPerShard: Int)
