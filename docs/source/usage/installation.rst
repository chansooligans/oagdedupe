Installation
----------------

To use oagdedupe, first install it using pip:

.. note::

   Not yet available -- git clone and install

.. code-block:: console

   pip install oagdedupe


label-studio
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start label-studio, e.g. on port 8089.

.. code-block:: console

   docker run -it -p 8089:8080 -v `pwd`/cache/mydata:/label-studio/data \
      --env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
      --env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
      -v `pwd`/cache/myfiles:/label-studio/files \
      heartexlabs/label-studio:latest label-studio

postgres
^^^^^^^^^^^^^^^^^^^^^^^^^^^

[insert instructions here about initializing postgres]

most importantly, need to create functions (dedupe/postgres/funcs.py)