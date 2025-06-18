# Stress Tests - High-Performance Load Testing

This document explains FLINK.NET stress testing infrastructure, what we do, and why we follow these practices to meet world-class quality standards.

## Overview

Our stress tests validate FLINK.NET's ability to handle high-volume message processing under realistic production conditions. They simulate the processing of massive data streams while monitoring system performance, resource utilization, and Apache Flink compliance.

## What We Do

### Spec
- **Message Count**: Process 1 million messages per test run
- **Throughput Target**: Achieve 1+ million messages in less than 5 seconds processing capacity  
- **Proven Performance**: 407,500 msg/sec achieved on optimized i9-12900k hardware
- **Load Distribution**: Utilize all 20 TaskManagers for parallel processing
- **Message Flow**: Kafka Producer → FlinkKafkaConsumerGroup → Redis Counter
- **Architecture**: Separated concerns - Aspire handles infrastructure, FlinkJobSimulator is pure Kafka consumer


## How to run
1/ Make sure Docker Desktop is running or the equivelant like containerd/Rancher Desktop.
![Docker Desktop](TestScreenshoots/Docker-Desktop.png)
2/ Open FLINK.NET\FlinkDotNetAspire\FlinkDotNetAspire.sln > F5
3/ Wait all the services running
![Aspire_Running](TestScreenshoots/Aspire_Running.png)
4/ `cd scripts` > Run `.\produce-1-million-messages.ps1`
![Open](TestScreenshoots/open-produce-1-million-messages.png)
![Run](TestScreenshoots/run-produce-1-million-messages.png)  
5/ Run `wait-for-flinkjobsimulator-completion.ps1`


This shows the Apache Flink processing pipeline with proper partition distribution and exactly-once semantics.

---
[Back to Wiki Home](Home.md)