Tweet Enrichment Real-Time Pipeline (Clean Architecture)

A production-grade streaming pipeline designed for real-time data processing, enrichment, and analytics using Apache Kafka and Apache Flink. This project strictly adheres to Domain-Driven Design (DDD) principles and Clean Architecture, ensuring the business logic remains decoupled from infrastructure concerns.

System ArchitectureThe project is organized into layers to maintain a high degree of testability and scalability:
src/domain/ (Core Layer): Contains the enterprise business rules.Entities: Tweet entity representing the core data structure.Value Objects: Company, Priority, and AuthorID to handle domain-specific validation and logic.Repository Interfaces: Abstract definitions for message producers and data storage.
src/application/ (Use Case Layer): Orchestrates the flow of data.StreamTweets: Logic for ingesting and producing raw events.ConsumeTweets: Logic for handling enriched data consumption.
src/infrastructure/ (Technical Implementation): External tools and frameworks.kafka/: Concrete implementations of Kafka Producers and Consumers.
flink/: Stream processing logic and PyFlink job definitions.
repositories/: Implementation of data persistence (PostgreSQL and CSV).
src/presentation/ (Entry Points):Launchers: producer_launcher.py and worker_launcher.py act as the application's execution bridge.
dags/ (Orchestration):Apache Airflow DAGs managing the entire lifecycle: environment health checks, topic creation, and job submission.

 Tech StackComponentTechnologyStream ProcessingApache Flink (PyFlink)Message BrokerApache Kafka & ZookeeperOrchestrationApache AirflowDatabasePostgreSQLContainerizationDocker & Docker Compose (Multi-stage builds)LanguagePython 3.10+

 Testing Strategy
The project features a comprehensive test suite located in /tests:
Unit Tests: Validating domain logic (Priority calculation, Company matching).
Integration Tests: Testing DB connections and Kafka message flow.
Smoke Tests: Ensuring configuration and imports are valid.
E2E Tests: End-to-end pipeline validation.