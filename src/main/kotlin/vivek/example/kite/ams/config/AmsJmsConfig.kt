package vivek.example.kite.ams.config

import jakarta.jms.ConnectionFactory
import jakarta.jms.Session
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jms.config.DefaultJmsListenerContainerFactory

@Configuration
class AmsJmsConfig {

  /**
   * A generic listener container factory for AMS shards. This bean provides the common baseline
   * configuration (e.g., pub/sub domain, shared subscription). The ShardManager will use this as a
   * template to create specific, customized listener containers for each shard at runtime.
   */
  @Bean("amsListenerContainerFactory")
  fun amsListenerContainerFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
  ): DefaultJmsListenerContainerFactory {
    val factory = DefaultJmsListenerContainerFactory()
    configurer.configure(factory, connectionFactory)
    factory.setPubSubDomain(true)
    factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE)
    factory.setSubscriptionShared(
        true) // Enable shared subscription for load balancing across real instances
    return factory
  }
}
