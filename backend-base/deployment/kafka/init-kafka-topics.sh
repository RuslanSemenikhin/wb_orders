#!/bin/bash

KT="/opt/bitnami/kafka/bin/kafka-topics.sh"

echo "Подключаемся к kafka"
"$KT" --bootstrap-server localhost:9092 --list

echo "Создаём топик"
"$KT" --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --replication-factor 1 --partitions 1

echo "Топики созданы:"
"$KT" --bootstrap-server localhost:9092 --list
