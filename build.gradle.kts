plugins {
    java
    id("org.springframework.boot") version "3.2.5" apply false
    id("io.spring.dependency-management") version "1.1.5" apply false
}

val springBootVersion by extra("3.2.5")
val kafkaClientVersion by extra("3.6.2")
val micrometerVersion by extra("1.12.5")
val jacksonVersion by extra("2.16.2")
val junit5Version by extra("5.10.2")
val testcontainersVersion by extra("1.19.8")
val awaitilityVersion by extra("4.2.1")
val assertjVersion by extra("3.25.3")

allprojects {
    group = "com.framework.resilient"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-parameters"))
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:${rootProject.extra["junit5Version"]}")
        testImplementation("org.assertj:assertj-core:${rootProject.extra["assertjVersion"]}")
    }
}
