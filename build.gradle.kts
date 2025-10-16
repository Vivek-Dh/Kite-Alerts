import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    kotlin("jvm") version "1.9.23"
    kotlin("plugin.spring") version "1.9.23"
    kotlin("plugin.jpa") version "1.9.23" // This plugin generates no-arg constructors for JPA entities
    id("org.springframework.boot") version "3.2.5"
    id("io.spring.dependency-management") version "1.1.5"
    id("com.diffplug.spotless") version "6.25.0"
}

group = "vivek.example.kite"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.8.1")

    implementation("org.springframework.boot:spring-boot-starter-webflux") // For SSE streaming API
    implementation("org.springframework.boot:spring-boot-starter-artemis")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    implementation("org.apache.activemq:artemis-jakarta-server")

    // Jackson for JSON serialization
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Library for a robust, non-cryptographic hash function (MurmurHash3)
    implementation("com.google.guava:guava:33.2.1-jre")

    // Library for handling JSONB types in Hibernate
    implementation("io.hypersistence:hypersistence-utils-hibernate-63:3.11.0")

    // RocksDB
    implementation("org.rocksdb:rocksdbjni:8.11.3")

    // PostgreSQL
    runtimeOnly("org.postgresql:postgresql")

    // Flyway for database migrations
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")

    // Test dependencies
    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "org.mockito")
    }
    testImplementation(platform("org.springframework.boot:spring-boot-dependencies:3.2.5"))
    testImplementation("io.projectreactor:reactor-test")
    // Awaitility for asynchronous testing
    testImplementation("org.awaitility:awaitility-kotlin:4.2.1")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testImplementation(kotlin("test"))
    testImplementation("com.h2database:h2")

    // Add Testcontainers for PostgreSQL integration testing
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:junit-jupiter")
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
}
kotlin {
    jvmToolchain(21)
}

springBoot {
    mainClass.set("vivek.example.kite.KiteAlertsApplicationKt")
}

spotless {
    kotlin {
        ktfmt() // Use ktfmt for Kotlin formatting
        trimTrailingWhitespace()
        endWithNewline()
    }
    kotlinGradle {
        ktlint()
        target("*.gradle.kts")
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// Make check depend on spotlessCheck
tasks.check {
    dependsOn(tasks.spotlessCheck)
}

// Make build depend on spotlessApply
tasks.build {
    dependsOn(tasks.spotlessApply)
}

tasks.withType<BootJar> {
    mainClass.set("vivek.example.kite.KiteAlertsApplicationKt")
    archiveFileName.set("${project.name}.jar")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs =
        listOf(
            "-XX:+EnableDynamicAgentLoading",
            "-Djdk.instrument.traceUsage=false",
            "-Dspring.test.constructor.autowire.mode=all",
        )
}

tasks.compileKotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_21)
        freeCompilerArgs = listOf("-Xjsr305=strict")
    }
}
