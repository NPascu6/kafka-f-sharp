# KafkaConfig and KafkaService README

This repository provides a F#-based configuration and service layer for interacting with an Apache Kafka cluster, utilizing the Confluent.Kafka library. It includes implementations for Kafka configuration, service actions like producing, consuming, and managing topics, and a logging wrapper to streamline log management.

## Table of Contents
- [Overview](#overview)
- [Components](#components)
- [Usage](#usage)
- [Kafka Docker Setup](#kafka-docker-setup)
- [Dependencies](#dependencies)

---

## Overview

The Kafka service implementation provides a high-level API for interacting with Kafka:
- Producing and consuming messages.
- Managing Kafka topics.
- Logging and error handling for message and topic operations.

The Docker Compose configuration in this repository sets up a Kafka cluster with three brokers and a Zookeeper instance, along with a Kafka UI for easier cluster management.

---

## Components

### 1. **`IKafkaConfig`**
Defines the Kafka configuration, specifying `BootstrapServers` and `SecurityProtocol`.

### 2. **`IKafkaService`**
An interface for Kafka services that includes methods to:
   - **Produce Messages**: Send messages to specific topics with optional partition selection.
   - **Manage Topics**: Create, delete, list, and retrieve topic details.
   - **Consume Messages**: Fetch messages from a topic using a specific consumer group.

### 3. **`ILoggingWrapper`**
Defines a logging interface for structured logging, with methods to log information, warnings, and errors.

### 4. **Concrete Implementations**
   - **`KafkaConfig`**: A concrete implementation of `IKafkaConfig` with default settings.
   - **`KafkaService`**: A Kafka service implementation that uses injected `IKafkaConfig` and `ILoggingWrapper` for handling Kafka actions.
   - **`LoggingWrapper`**: Uses Serilog for logging.

---

## Usage

### KafkaClient Program

The main program file provides an interactive CLI to test the functionality of the `KafkaService` with options to:
- Create topics.
- Send messages.
- List, delete, and get details of topics.
- Consume messages from a topic.

#### Running the CLI
To start the program:
1. Ensure your Kafka cluster is running (see [Kafka Docker Setup](#kafka-docker-setup) below).
2. Run the program:
   ```shell
   dotnet run

CLI Options
Upon running, you will see the following options:

1: Create a new topic.
2: Send a message to a topic.
3: List available topics.
4: Delete a topic.
5: Get details for a topic.
6: Consume messages from a topic.
q: Quit the application.
Kafka Docker Setup
The Docker Compose file (docker-compose.yml) in this repository configures a local Kafka cluster with Zookeeper and Kafka UI.

Services
zookeeper: Manages broker metadata.
kafka1, kafka2, kafka3: Three Kafka brokers.
kafka-ui: A web interface for managing Kafka topics, partitions, consumers, etc.
Running the Docker Setup
Start the services:
shell
Copy code
docker-compose up -d
Access Kafka UI at http://localhost:8080 to manage topics and view cluster status.
Ports
Kafka Brokers: 29092, 29093, 29094 (for external access)
Kafka UI: 8080
Volumes
Each broker's data is persisted using Docker volumes for data durability across restarts.

Dependencies
Confluent.Kafka: Kafka client library.
Serilog: Logging framework for .NET.
Docker and Docker Compose: For setting up the Kafka cluster.
Dotnet SDK: .NET 5.0 or higher.
License
This project is open-source. Feel free to modify and distribute it.
