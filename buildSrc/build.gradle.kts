plugins {
    `kotlin-dsl`

//    base
//    java
//    `java-test-fixtures`
//
//    scala
//
//    idea
//
//    `maven-publish`
//
//    id("com.github.ben-manes.versions" ) version "0.42.0"
}

repositories {
    mavenLocal()
    mavenCentral()
//    jcenter()
    maven("https://dl.bintray.com/kotlin/kotlin-dev")
    gradlePluginPortal() // so that external plugins can be resolved in dependencies section
}

dependencies {

    implementation("com.github.ben-manes.versions:com.github.ben-manes.versions.gradle.plugin:0.49.0")
    implementation("io.github.gradle-nexus.publish-plugin:io.github.gradle-nexus.publish-plugin.gradle.plugin:1.3.0")
    implementation("io.github.cosmicsilence.scalafix:io.github.cosmicsilence.scalafix.gradle.plugin:0.1.14")
}