# Environment Setup Documentation

## Architecture Overview

The project environment is containerized using **Docker** and managed via **Docker Compose**. This ensures a consistent, reproducible, and isolated Big Data processing environment without requiring complex local installations of Hadoop or Spark.

The architecture consists of a cluster of containers running on a single host machine (Devcontainer setup).

### Containerized Services
The `docker-compose.yml` defines the following services connected via the `bigdata-net` bridge network:

1.  **Hadoop HDFS Layer**
    *   **Namenode (`namenode`)**: The master node for HDFS, managing file system metadata.
        *   Image: `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`
        *   Ports: `9870` (Web UI), `9000` (IPC)
    *   **Datanode (`datanode`)**: The worker node for HDFS, storing actual data blocks.
        *   Image: `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`
        *   Ports: `9864`
        *   Depends on: `namenode`

2.  **Apache Spark Layer**
    *   **Spark Master (`spark-master`)**: The coordinator for the Spark application.
        *   Image: `jupyter/pyspark-notebook:spark-3.5.0`
        *   Ports: `8080` (Web UI), `7077` (Master Port)
    *   **Spark Worker (`spark-worker`)**: Executes tasks assigned by the master.
        *   Image: `jupyter/pyspark-notebook:spark-3.5.0`
        *   Resources: Configured with `4` cores and `8GB` memory.
        *   Depends on: `spark-master`
    *   **Spark Client (`spark-client`)**: The development container where code is edited and submitted.
        *   Image: `jupyter/pyspark-notebook:spark-3.5.0`
        *   Mounts: The current project directory is mounted to `/workspace`.
        *   Environment: `PYSPARK_PYTHON=python3`, `SPARK_MASTER_URL=spark://spark-master:7077`.

---

## Host Machine Specifications

The containers run on the following personal computer configuration:

### Hardware Configuration
- **System:** ASUS ROG Zephyrus G16 GU605MI_GU605MI
- **CPU:** Intel Core Ultra 7 155H
  - **Cores:** 16 physical cores
  - **Logical Processors:** 22 threads (with Hyper-Threading)
  - **Base Clock:** ~1.4 GHz
  - **Max Clock Speed:** 3.8 GHz
- **Memory:** 32GB (32,189 MB) Physical RAM
- **Architecture:** x64-based PC

### Software Environment (Host)
- **OS:** Windows 11
- **Container Engine:** Docker Desktop for Windows (WSL 2 Backend)
- **IDE:** Visual Studio Code with Dev Containers extension