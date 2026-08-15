pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "kafka-resilient-consumer"

include("framework")
include("demo")
include("benchmarks")
