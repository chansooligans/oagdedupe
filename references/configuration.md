# Configuration

Deduper requires a config file, which is loaded when deduper is initialized, e.g. `import dedupe`.

Currently, a distinct configuration file is required for each dedupe application (issue created: https://github.com/chansooligans/deduper/issues/28)

There are two options:

(1) A global config file at ~/.dedupe/config.ini
(2) A per-project config file at $CWD/.dedupe/config.ini, where $CWD is the folder you're running your script from (the folder where you `import dedupe`).

The config file must be called `config.ini` contain:

```
[MAIN]
HOST = http://172.22.39.26
MODEL_FILEPATH = [path to model to load, or save if it does not exist]
CACHE_FILEPATH = [path to store database files for SQLite and comparison pairs]

[LABEL_STUDIO]
PORT = 8001
API_KEY = [API KEY from Label Studio interface after signing up / logging in]
TITLE = [Title of Dedupe Project]
DESCRIPTION = [Description of Project]

[FAST_API]
PORT = 8000
CPUS = [Number of CPUs for distance calcs and ML models]
```