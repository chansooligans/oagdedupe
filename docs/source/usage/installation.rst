Installation
----------------

To use oagdedupe, first install it using pip:

.. code-block:: console

   pip install oagdedupe

.dedupe
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a cache folder to store postgres and labelstudio data.

```
mkdir .dedupe
```

docker
^^^^^^^^^^^^^^^^^^^^^^^^^^^

We use docker to run postgres and label-studio. This is the msot convenient, 
but it's not necessary and they can be installed however way you want. 
The only requirement is that the running postgres database have `plpython3`
installed.

If you do not already have docker installed, see: 
https://docs.docker.com/get-started/#download-and-install-docker


postgres
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start postgres using docker, updating `[POSTGRES_PORT]` to the 
port on your host machine

.. code-block:: console

   docker run --rm -dp [POSTGRES_PORT]:5432 \
      --name oagdedupe-postgres \
      --env POSTGRES_USER=username \
      --env POSTGRES_PASSWORD=password \
      --env POSTGRES_DB=db \
      --env PGDATA=/var/lib/pgsql/data/pgdata \
      -v `pwd`/.dedupe:/var/lib/pgsql/data \
      postgres:latest

label-studio
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start label-studio using docker, updating `[LS_PORT]` to the 
port on your host machine

.. code-block:: console

   docker run -it -p [LS_PORT]:8080 -v `pwd`/cache/mydata:/label-studio/data \
      --env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
      --env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
      -v `pwd`/.dedupe:/label-studio/files \
      heartexlabs/label-studio:latest label-studio
