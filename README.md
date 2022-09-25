# oagdedupe  

oagdedupe is a Python library for scalable entity resolution, using active 
learning to learn blocking configurations, generate comparison pairs, 
then clasify matches. 

## page contents
- [Documentation](#documentation)
- [Installation](#installation)
    - [label-studio](#label-studio)
    - [postgres](#postgres)
    - [project settings](#project-settings)
- [dedupe](#dedupe-example)
- [record-linkage](#record-linkage-example)
    
# Documentation<a name="#documentation"></a>

You can find the documentation of oagdedupe at https://deduper.readthedocs.io/en/latest/, 
where you can find [Getting started](https://deduper.readthedocs.io/en/latest/usage/installation.html), 
the [api reference](https://deduper.readthedocs.io/en/latest/dedupe/api.html), 
[guide to methodology](https://deduper.readthedocs.io/en/latest/userguide/intro.html),
and [examples](https://deduper.readthedocs.io/en/latest/examples/example_dedupe.html).

# Installation<a name="#installation"></a>

```
# PyPI
pip install oagdedupe
```

## label-studio<a name="#label-studio"></a>

Start label-studio using docker command below, updating `[LS_PORT]` to the 
port on your host machine

```
docker run -it -p [LS_PORT]:8080 -v `pwd`/cache/mydata:/label-studio/data \
	--env LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true \
	--env LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/label-studio/files \
	-v `pwd`/cache/myfiles:/label-studio/files \
	heartexlabs/label-studio:latest label-studio
```

## postgres<a name="#postgres"></a>

[insert instructions here about initializing postgres]

most importantly, need to create functions (dedupe/postgres/funcs.py)
