Installation
----------------

To use oagdedupe, first install it using pip:

.. code-block:: console

   pip install oagdedupe

.deduper
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a cache folder to store postgres and labelstudio data.

```
mkdir .deduper
```

postgres
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a new file called `Dockerfile` with:

.. code-block:: console
   
   FROM postgres:latest

   RUN apt-get update
   RUN apt-get -y install python3 postgresql-plpython3-14

Then build and run:

.. code-block:: console

   docker build -t oagdedupe-postgres . ;
   docker run -dp 8000:5432 \
      --env POSTGRES_USER=username \
      --env POSTGRES_PASSWORD=password \
      --env POSTGRES_DB=db \
      --env PGDATA=/var/lib/pgsql/data/pgdata \
      -v `pwd`/.deduper:/var/lib/pgsql/data \
      postgres 

label-studio
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start label-studio using docker command below, updating `[LS_PORT]` to the 
port on your host machine

.. code-block:: console

   docker run -it -p 8089:8080 -v `pwd`/cache/mydata:/label-studio/data \
      --env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
      --env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
      -v `pwd`/cache/myfiles:/label-studio/files \
      heartexlabs/label-studio:latest label-studio