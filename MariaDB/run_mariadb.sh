#!/bin/bash
docker run -d \
  --name mariadb_benchmark \
  -p 3307:3306 \
  -e MARIADB_ROOT_PASSWORD=password \
  -e MARIADB_DATABASE=ecommerce_benchmark \
  -v "$HOME/docker_volumes/sql/mariadb:/var/lib/mysql" \
  mariadb:latest

echo "MariaDB uruchomiony na porcie 3307"