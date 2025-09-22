# AGENTS.md

This file contains information for AI agents working on the SpookyStuff project.

## Project Overview

**SpookyStuff** - Distributed Agent Swarm

- **Language**: Scala 2.13
- **Build System**: Gradle with Kotlin DSL
- **Architecture**: Multi-module project with the following modules:
    - `core` - Core functionality
    - `web` - Browser control and web scraping (Beta)
    - `commons` - Common utilities
    - `linq` - Language integrated query functionality
    - `assembly` - Assembly and packaging
    - `benchmark` - Performance benchmarking
    - `integration` - Integration tests
    - `sanity` - Sanity tests
    - `prover-commons` - Mathematical proving utilities (submodule)
- **Main Technologies**: Apache Spark, Apache Tika, Selenium, PhantomJS
- **Main Package**: `ai.acyclic` (likely based on build files)

## Frequently Used Commands

### Build & Compile

```bash
./gradlew build                # Build all modules
./gradlew clean                # Clean build directories
./gradlew compileScala         # Compile main Scala sources
./gradlew compileTestScala     # Compile test Scala sources
```

### Testing

```bash
./gradlew test                 # Run all tests
./gradlew check                # Run all checks (includes tests)
./gradlew testClasses          # Compile test classes
```

### Code Quality & Formatting

```bash
./gradlew scalafix             # Apply Scalafix rules
./gradlew checkScalafix        # Check Scalafix compliance (read-only)
./gradlew scalafixMain         # Apply Scalafix to main sources
./gradlew scalafixTest         # Apply Scalafix to test sources
```

### Module-Specific Commands

```bash
./gradlew :core:build          # Build core module
./gradlew :web:test           # Test web module  
./gradlew :commons:scalafix   # Fix commons module
./gradlew :prover-commons:build # Build prover-commons submodule
```

### Assembly & Packaging

```bash
./gradlew shadowJar           # Create fat JAR with dependencies
./gradlew :assembly:build     # Build assembly module
```

## Code Style & Conventions

### Package Structure

- Base package: `ai.acyclic`
- Module-specific packages follow standard conventions
- SpookyStuff-specific functionality likely under `ai.acyclic.spookystuff`
- Prover commons utilities under `ai.acyclic.prover.commons`
- Do not modify gradle file unless explicitly asked to so

### Scala Conventions

- Uses Scala 2.13 with modern language features
- Imports are grouped and organized (java, scala, third-party, local)
- Case classes are preferred for data structures
- Implicit conversions are used for DSL-like syntax
- Uses `@transient` annotations for non-serializable fields in serializable classes
- Apache Spark integration patterns throughout
- do not use "???"

### File Organization

- Source files: `module/{module-name}/src/main/scala/`
- Test files: `module/{module-name}/src/test/scala/`
- Test fixtures: `module/{module-name}/src/testFixtures/scala/`

### Dependencies

- **Apache Spark** - Core distributed computing framework
- **Apache Tika** - Content analysis and metadata extraction
- **Selenium** - Web browser automation
- **PhantomJS** - Headless web browser (deprecated, likely legacy)
- **Scalafmt** - Code formatting
- **Shadow plugin** - Fat JAR creation

## Project Components

### Web Module (Beta)

- Browser control and automation
- Web scraping capabilities
- Selenium integration

### UAV Module (Work in Progress)

- Unmanned system control
- MAVLink protocol support

### Core Module

- Distributed processing with Apache Spark
- Core SpookyStuff functionality

### Prover Commons (Submodule)

- Mathematical utilities and proving
- Separate Git submodule with own build system

## Testing Guidelines

- Test fixtures are shared across modules using `testFixtures` source set
- Integration tests in dedicated `integration` module
- Sanity tests in dedicated `sanity` module
- Use appropriate test location: unit tests in module tests, integration tests in integration module

## Development Workflow

1. Always run `./gradlew check` before committing to ensure code quality
2. Use `./gradlew scalafix` to automatically apply code style fixes
3. Build specific modules during development: `./gradlew :{module}:build`
4. Run `./gradlew checkScalafix` in CI to verify code style compliance
5. Use `./gradlew shadowJar` to create deployable artifacts

## Configuration Files

- `.scalafix.conf` - Scalafix configuration for code style rules
- `.scalafmt.conf` - Scalafmt configuration for code formatting
- `build.gradle.kts` - Main build configuration
- `settings.gradle.kts` - Multi-module project settings
- `gradle.properties` - Gradle build properties
- `.gitmodules` - Git submodule configuration (prover-commons)

## Special Notes

- This is a complex distributed system project with web automation capabilities
- Contains both stable (web) and experimental (UAV) components
- Uses Apache Spark for distributed processing
- Has CI/CD integration with Codeship
- Prover-commons is a Git submodule - changes there require separate commits
