plugins {
    `java-library`
    id("io.spring.dependency-management")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${rootProject.extra["springBootVersion"]}")
    }
}

dependencies {
    // Spring Boot (library only - no application plugin)
    api("org.springframework.boot:spring-boot-starter")
    api("org.springframework.boot:spring-boot-starter-actuator")

    // Kafka
    api("org.apache.kafka:kafka-clients:${rootProject.extra["kafkaClientVersion"]}")

    // Micrometer (metrics)
    api("io.micrometer:micrometer-core:${rootProject.extra["micrometerVersion"]}")
    api("io.micrometer:micrometer-registry-prometheus:${rootProject.extra["micrometerVersion"]}")

    // Jackson (serialization)
    implementation("com.fasterxml.jackson.core:jackson-databind:${rootProject.extra["jacksonVersion"]}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${rootProject.extra["jacksonVersion"]}")

    // Structured JSON logging
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")

    // Annotation processing for @ConfigurationProperties
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    // Redis (optional — for RedisIdempotencyKeyStore and RedisStateStore)
    compileOnly("org.springframework.boot:spring-boot-starter-data-redis")
    testImplementation("org.springframework.boot:spring-boot-starter-data-redis")

    // Test dependencies
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility:${rootProject.extra["awaitilityVersion"]}")
    testImplementation("org.testcontainers:testcontainers:${rootProject.extra["testcontainersVersion"]}")
    testImplementation("org.testcontainers:kafka:${rootProject.extra["testcontainersVersion"]}")
    testImplementation("org.testcontainers:junit-jupiter:${rootProject.extra["testcontainersVersion"]}")
}
