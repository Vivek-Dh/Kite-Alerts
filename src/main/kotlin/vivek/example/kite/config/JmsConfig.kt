package vivek.example.kite.config

// import org.apache.activemq.ActiveMQConnectionFactory
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.jms.ConnectionFactory
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jms.annotation.EnableJms
import org.springframework.jms.config.DefaultJmsListenerContainerFactory
import org.springframework.jms.core.JmsTemplate
import org.springframework.jms.support.converter.MappingJackson2MessageConverter
import org.springframework.jms.support.converter.MessageConverter
import org.springframework.jms.support.converter.MessageType

@Configuration
@EnableJms
class JmsConfig {

  // Configure Jackson message converter to send/receive JSON payloads
  @Bean
  fun jacksonJmsMessageConverter(objectMapper: ObjectMapper): MessageConverter {
    val converter = MappingJackson2MessageConverter()
    converter.setTargetType(MessageType.TEXT)
    converter.setTypeIdPropertyName("_type")
    converter.setObjectMapper(objectMapper)
    return converter
  }

  // A specific JmsTemplate for publishing to Topics
  @Bean
  fun jmsTopicTemplate(
      connectionFactory: ConnectionFactory,
      messageConverter: MessageConverter
  ): JmsTemplate {
    val template = JmsTemplate(connectionFactory)
    template.messageConverter = messageConverter
    template.isPubSubDomain = true // This is crucial for sending to topics
    return template
  }

  // A generic factory for topic listeners
  private fun topicListenerFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      concurrency: String
  ): DefaultJmsListenerContainerFactory {
    val factory = DefaultJmsListenerContainerFactory()
    configurer.configure(factory, connectionFactory)
    factory.setConcurrency(concurrency)
    factory.setPubSubDomain(true) // This is crucial for listening to topics
    return factory
  }

  // Listener factories for different categories, allowing different concurrency on topics
  @Bean
  fun highActivityFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      appProperties: AppProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory,
        configurer,
        appProperties.windowAggregator.listenerConcurrency["HIGH_ACTIVITY"]!!)
  }

  @Bean
  fun mediumActivityFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      appProperties: AppProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory,
        configurer,
        appProperties.windowAggregator.listenerConcurrency["MEDIUM_ACTIVITY"]!!)
  }

  @Bean
  fun lowActivityFactory(
      connectionFactory: ConnectionFactory,
      configurer: DefaultJmsListenerContainerFactoryConfigurer,
      appProperties: AppProperties
  ): DefaultJmsListenerContainerFactory {
    return topicListenerFactory(
        connectionFactory,
        configurer,
        appProperties.windowAggregator.listenerConcurrency["LOW_ACTIVITY"]!!)
  }
}
