FROM postgres:14

RUN apt-get update
RUN apt-get -y install python3 python3-pip postgresql-plpython3-14 git make gcc 
RUN apt-get -y install postgresql-server-dev-14 postgresql-14-pgrouting

RUN git clone https://github.com/eulerto/pg_similarity.git \
 && cd pg_similarity \
 && ls \
 && USE_PGXS=1 make \
 && USE_PGXS=1 make install