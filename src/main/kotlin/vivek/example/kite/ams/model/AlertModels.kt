package vivek.example.kite.ams.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import io.hypersistence.utils.hibernate.type.json.JsonType
import jakarta.persistence.*
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import org.hibernate.annotations.Generated
import org.hibernate.annotations.GenerationTime
import org.hibernate.annotations.Type
import vivek.example.kite.tickprocessor.model.AggregatedLHWindow

enum class ConditionType {
  GT,
  GTE,
  LT,
  LTE,
  EQ
}

@Entity
@Table(name = "alerts")
data class Alert(
    @Id val id: UUID = UUID.randomUUID(),
    val alertId: String,
    val stockSymbol: String,
    val userId: String,
    @Column(precision = 10, scale = 2) val priceThreshold: BigDecimal,
    @Enumerated(EnumType.STRING) val conditionType: ConditionType,
    var isActive: Boolean = true,
    @Generated(GenerationTime.INSERT)
    @Column(name = "created_at", insertable = false, updatable = false)
    @Temporal(TemporalType.TIMESTAMP)
    var createdAt: Instant? = null,
    @Generated(GenerationTime.ALWAYS)
    @Column(name = "updated_at", insertable = false, updatable = false)
    @Temporal(TemporalType.TIMESTAMP)
    var updatedAt: Instant? = null
) {

  @get:Transient
  @get:Temporal(TemporalType.TIMESTAMP)
  @get:JsonProperty(access = JsonProperty.Access.READ_ONLY)
  val createdAtUtc: LocalDateTime?
    get() = createdAt?.atZone(ZoneOffset.UTC)?.toLocalDateTime()

  @get:Transient
  @get:Temporal(TemporalType.TIMESTAMP)
  @get:JsonProperty(access = JsonProperty.Access.READ_ONLY)
  val updatedAtUtc: LocalDateTime?
    get() = updatedAt?.atZone(ZoneOffset.UTC)?.toLocalDateTime()

  @JsonIgnore
  fun getLogKey(): String {
    return "${alertId}_${userId}_$id"
  }
}

@Entity
@Table(name = "alert_outbox_events")
data class AlertOutboxEvent(
    @Id val id: UUID = UUID.randomUUID(),
    @Type(JsonType::class) @Column(columnDefinition = "jsonb") val payload: AlertChangeEvent,

    // Changed from Long? to Instant?
    @Column(name = "processed_at", insertable = false)
    @Temporal(TemporalType.TIMESTAMP)
    var processedAt: Instant? = null
) {
  @Column(insertable = false, updatable = false)
  @Temporal(TemporalType.TIMESTAMP)
  var createdAt: Instant? = null
}

// New Entity to store the history of triggered alerts
@Entity
@Table(name = "triggered_alerts_history")
data class TriggeredAlertHistory(
    @Id val id: UUID = UUID.randomUUID(),
    val alertId: UUID,
    val userId: String,
    val stockSymbol: String,
    @Column(precision = 10, scale = 2) val triggeredPrice: BigDecimal,
    @Temporal(TemporalType.TIMESTAMP) val triggeredAt: Instant,
    @Type(JsonType::class) @Column(columnDefinition = "jsonb") val alertPayload: Alert
) {
  @Column(insertable = false, updatable = false)
  @Temporal(TemporalType.TIMESTAMP)
  var createdAt: Instant? = null
}

// Not an entity, this is the JSON payload for the outbox event
data class AlertChangeEvent(
    val alert: Alert,
    val eventType: String = "CREATE" // Default to CREATE, can be UPDATE or DELETE
)

data class AlertRequest(
    val stockSymbol: String,
    val userId: String,
    val priceThreshold: BigDecimal,
    val conditionType: ConditionType
)

data class AlertDetail(
    val id: UUID,
    val alertId: String,
    val conditionType: ConditionType,
    val priceThreshold: BigDecimal,
    val isActive: Boolean
)

data class TriggeredAlertEvent(
    val eventId: String,
    val alert: Alert,
    val triggeredAt: Instant,
    val triggeredPrice: BigDecimal,
    val window: AggregatedLHWindow,
    val message: String
) {
  @Transient
  var triggeredAtUtc: LocalDateTime = triggeredAt.atZone(ZoneOffset.UTC).toLocalDateTime()
}
