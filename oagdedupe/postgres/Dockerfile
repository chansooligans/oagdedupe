FROM postgres:14

RUN apt-get update
RUN apt-get -y install python3 postgresql-plpython3-14 git make gcc 
RUN apt-get -y install postgresql-server-dev-14

RUN git clone https://github.com/eulerto/pg_similarity.git \
 && cd pg_similarity \
 && ls \
 && USE_PGXS=1 make \
 && USE_PGXS=1 make install