#!/bin/sn

# Uruchomienie Redis
docker run -d \
  --name redis_benchmark \
  -p 6379:6379 \
  -v "$(pwd)/data/redis:/data" \
  redis:latest \
  redis-server --save 60 1 --loglevel warning

echo "Redis uruchomiony na porcie 6379"
