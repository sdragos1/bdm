## Installation Guide

This repository will serve for all Big Data Mining related projects. Below are the steps to set up the environment and install necessary dependencies.

### Spark Setup

Spark & Hadoop are exactly installed as explained during class presentation.

The only bonus thing I added is a HDFS setup on the Raspberry Pi 5.

### Running the project

Install requirements:
```bash
   pip install -r requirements.txt
```

Run the spark & hadoop setup as specified in the ENVIRONMENT_SETUP.md file and during class.

And then submit the job:
```bash
spark-submit --master spark://<IP>:7077 --conf spark.driver.host=<IP>  .\household_power_consumption\task3\task3.py
```
