docker-compose rm -fsv
docker-compose -f dedupe/postgres/docker-compose.yml up -d --remove-orphan

# docker run \
#   --rm -d -p 8000:5432 \
#   --name postgres-oaglib \
#   -e POSTGRES_USER="username" \
#   -e POSTGRES_PASSWORD="password" \
#   -e POSTGRES_DB="db" \
#   -e PGDATA=/var/lib/postgresql/data/pgdata \
#   -v /home/csong/team_repos/blocker/db:/var/lib/postgresql/data \
#   postgres