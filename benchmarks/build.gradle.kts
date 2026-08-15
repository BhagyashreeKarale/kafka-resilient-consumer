plugins {
    id("io.spring.dependency-management")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${rootProject.extra["springBootVersion"]}")
    }
}

dependencies {
    implementation(project(":framework"))

    // Kafka client for benchmark harness
    implementation("org.apache.kafka:kafka-clients:${rootProject.extra["kafkaClientVersion"]}")

    // Jackson for report generation
    implementation("com.fasterxml.jackson.core:jackson-databind:${rootProject.extra["jacksonVersion"]}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${rootProject.extra["jacksonVersion"]}")

    // Test dependencies (benchmarks run as JUnit 5 tests)
    testImplementation("org.junit.jupiter:junit-jupiter:${rootProject.extra["junit5Version"]}")
    testImplementation("org.assertj:assertj-core:${rootProject.extra["assertjVersion"]}")
    testImplementation("org.awaitility:awaitility:${rootProject.extra["awaitilityVersion"]}")

    // Testcontainers for isolated Kafka cluster
    testImplementation("org.testcontainers:testcontainers:${rootProject.extra["testcontainersVersion"]}")
    testImplementation("org.testcontainers:kafka:${rootProject.extra["testcontainersVersion"]}")
    testImplementation("org.testcontainers:junit-jupiter:${rootProject.extra["testcontainersVersion"]}")
}
