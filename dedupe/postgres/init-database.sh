#!/bin/bash
set -e


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    create extension pg_trgm;
    create extension plpython3u;
    create language pg_trgm;
    create language plpython3u;
EOSQL