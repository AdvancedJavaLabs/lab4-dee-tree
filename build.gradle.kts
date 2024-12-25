plugins {
    kotlin("jvm") version "2.0.21"
}

group = "edu.sokolov"
version = "unspecified"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-client:3.4.1")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}


val fatJar = task("fatJar", type = Jar::class) {
    archiveBaseName = "${project.name}-fat"
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Implementation-Version"] = version
        attributes["Main-Class"] = "edu.sokolov.Runner"
    }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    with(tasks.jar.get() as CopySpec)
}

tasks {
    "build" {
        dependsOn(fatJar)
    }
}

tasks.withType<org.gradle.jvm.tasks.Jar>() {
    exclude("META-INF/BC1024KE.RSA", "META-INF/BC1024KE.SF", "META-INF/BC1024KE.DSA")
    exclude("META-INF/BC2048KE.RSA", "META-INF/BC2048KE.SF", "META-INF/BC2048KE.DSA")
}