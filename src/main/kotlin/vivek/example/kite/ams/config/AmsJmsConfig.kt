package vivek.example.kite.ams.config

import jakarta.jms.ConnectionFactory
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jms.config.DefaultJmsListenerContainerFactory
import vivek.example.kite.config.JmsConfig

@Configuration
class AmsJmsConfig {

  @Bean
  fun amsListenerFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      amsProperties: AmsProperties
  ): DefaultJmsListenerContainerFactory {
    return JmsConfig.Factory.topicListenerFactory(
        connectionFactory, configurer, amsProperties.listenerConcurrency)
  }
}
