FROM postgres:latest

RUN apt-get update
RUN apt-get -y install python3 postgresql-plpython3-14

# Copy in the load-extensions script
COPY dedupe/postgres/init-database.sh /docker-entrypoint-initdb.d/