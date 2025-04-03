#!/bin/bash
docker run -d \
  --name mysql_benchmark \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=ecommerce_benchmark \
  -v "$HOME/docker_volumes/sql/mysql:/var/lib/mysql" \
  -v "$(pwd)/schema_mysql.sql:/docker-entrypoint-initdb.d/schema.sql:ro" \
  mysql:latest

echo "MySQL uruchomiony na porcie 3306"