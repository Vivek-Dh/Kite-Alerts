package vivek.example.kite.tickprocessor.config

import jakarta.jms.ConnectionFactory
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jms.config.DefaultJmsListenerContainerFactory
import vivek.example.kite.config.JmsConfig.Factory.topicListenerFactory

@Configuration
class TickProcessorJmsConfig {
  // Listener factories for different categories, allowing different concurrency on topics
  @Bean
  fun highActivityFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      tickProcessorProperties: TickProcessorProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory,
        configurer,
        tickProcessorProperties.windowAggregator.listenerConcurrency["HIGH_ACTIVITY"]!!)
  }

  @Bean
  fun mediumActivityFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      tickProcessorProperties: TickProcessorProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory,
        configurer,
        tickProcessorProperties.windowAggregator.listenerConcurrency["MEDIUM_ACTIVITY"]!!)
  }

  @Bean
  fun lowActivityFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      tickProcessorProperties: TickProcessorProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory,
        configurer,
        tickProcessorProperties.windowAggregator.listenerConcurrency["LOW_ACTIVITY"]!!)
  }

  @Bean
  fun priceStreamListenerFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      tickProcessorProperties: TickProcessorProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory, configurer, tickProcessorProperties.priceStream.listenerConcurrency)
  }
}
