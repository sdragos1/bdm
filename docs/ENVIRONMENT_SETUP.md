# Environment Setup Documentation

## Architecture Overview

This project implements a distributed big data processing system with the following architecture:
- **Master Node:** Windows 11 machine running Apache Spark and Python applications
- **Worker Node:** Raspberry Pi 5 running Hadoop HDFS for distributed storage and serves as a worker node

---

## Master Node Specifications

### Hardware Configuration
- **System:** ASUS ROG Zephyrus G16 GU605MI_GU605MI
- **CPU:** Intel Core Ultra 7 155H
  - **Cores:** 16 physical cores
  - **Logical Processors:** 22 threads (with Hyper-Threading)
  - **Base Clock:** ~1.4 GHz
  - **Max Clock Speed:** 3.8 GHz
- **Memory:** 32,189 MB (31.4 GB) Total Physical RAM
  - **Available:** ~12.6 GB (varies with system load)
- **Architecture:** x64-based PC
- **System Type:** Multiprocessor Free

### Software Environment
- **Java Runtime:** Oracle Java SE 17.0.12 LTS
  - **Build:** 17.0.12+8-LTS-286
  - **VM:** Java HotSpot 64-Bit Server VM
- **Python Environment:**
  - **Version:** Python 3.13.7
  - **Package Manager:** uv (modern Python package manager)
  - **Virtual Environment:** `.venv/` (project-specific)
- **Apache Spark:**
  - **Version:** PySpark 4.0.1+


## Worker Node Specifications (Raspberry Pi 5)

### Hardware Configuration
- **Device:** Raspberry Pi 5
- **CPU:** ARM Cortex-A76 quad-core
  - **Architecture:** 64-bit ARM (aarch64)
  - **Clock Speed:** Up to 2.4 GHz
- **Memory:** 4GB/8GB LPDDR4X SDRAM (model dependent)
- **Storage:** MicroSD card (minimum 32GB recommended)
- **Network:** Gigabit Ethernet + Wi-Fi 6

### Software Environment
- **Operating System:** Raspberry Pi OS Lite
  - **Base:** Debian 12 (Bookworm)
  - **Architecture:** ARM64
  - **Kernel:** Linux (latest stable)
- **Java Runtime:** OpenJDK (for Hadoop compatibility)
- **Hadoop Ecosystem:**
  - **Hadoop HDFS:** Distributed file system
  - **Configuration:** Single-node cluster mode
---

## Project Dependencies

### Python Dependencies (pyproject.toml)
```toml
[project]
name = "bdm"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "pyspark>=4.0.1",
]
```

### System Requirements
- **Master Node:**
  - Windows 11 (minimum Windows 10)
  - Java 17+ LTS
  - Python 3.13+
  - Minimum 16GB RAM (32GB recommended)
  - Multi-core processor (8+ cores recommended)
  
- **Worker Node:**
  - Raspberry Pi OS Lite
  - Java 11+ (OpenJDK)
  - Minimum 4GB RAM (8GB recommended)
  - Fast microSD card (Class 10, A2 rating)

---

## Network Architecture

### Communication Flow
```
Master Node (Windows 11)           Worker Node (Raspberry Pi 5)
├── Spark Driver                   ├── HDFS DataNode
├── Python Applications      <-->  ├── Hadoop Services
├── Data Processing               ├── Distributed Storage
└── Job Coordination              └── Data Replication
```
of the distributed big data processing environment. For specific installation and configuration procedures, refer to the accompanying setup guides.*