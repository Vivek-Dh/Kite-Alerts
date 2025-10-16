package vivek.example.kite.ams.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "ams.rocks-db")
data class RocksDbConfig(var basePath: String = "/mnt/c/kite_alerts/rocksdb")
