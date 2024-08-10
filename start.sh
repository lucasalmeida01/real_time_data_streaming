#!/bin/bash

# Instala o cassandra-driver usando pip
pip install cassandra-driver

# Executa o comando original do Spark
exec bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
