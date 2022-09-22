docker-compose rm -fsv
docker-compose -f dedupe/postgres/docker-compose.yml up -d --remove-orphan