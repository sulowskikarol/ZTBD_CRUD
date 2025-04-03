#!/bin/sh

# Tworzenie kontenera mongo
docker run -d \
  --name mongo_benchmark \
  -p 27017:27017 \
  -v ~/mongo-data:/data/db \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  mongo:latest

echo "MongoDB uruchomione na porcie 27017 z podłączonym katalogiem mongo_data w katalogu domowym użytkownika."
