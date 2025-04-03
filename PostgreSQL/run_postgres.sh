#!/bin/bash
docker run -d \
  --name postgres_benchmark \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_USER=admin \
  -e POSTGRES_DB=ecommerce_benchmark \
  -v "$HOME/docker_volumes/sql/postgres:/var/lib/postgresql/data" \
  postgres:latest

echo "PostgreSQL uruchomiony na porcie 5432"
