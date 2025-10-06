package vivek.example.kite.common.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@Configuration
@EnableScheduling // Enables the @Scheduled annotation for our timer
class SchedulerConfig {
  @Bean("windowAggregatorTaskExecutor")
  fun taskExecutor(): ThreadPoolTaskExecutor {
    val executor = ThreadPoolTaskExecutor()
    executor.corePoolSize = 10
    executor.maxPoolSize = 20
    executor.queueCapacity = 100
    executor.setThreadNamePrefix("window-processor-")
    executor.initialize()
    return executor
  }
}
