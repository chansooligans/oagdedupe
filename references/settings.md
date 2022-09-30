# Settings

To create a Settings object, you need to always specify name (the name of the project as a unique identifier) and folder (the folder location to save/read the settings file), or set the environment variables DEDUPER_NAME and DEDUPER_FOLDER. 

If a settings file already exists, you can read in all other settings with the read function. If not, you can create and save your settings to file with the save function. 

You can specify some or all settings when you create a Settings object, or set settings later with the set function. With the sync function you can either save settings to file if it doesn't exist or if you specified other settings, or read settings from file if it does exist.

#### Example of creating settings:

```
attributes = ["givenname", "surname", "suburb", "postcode"]


settings = Settings(
    name="test",  # the name of the project, a unique identifier
    folder="./.dedupe",  # path to folder where settings and data will be saved
    other=SettingsOther(
        cpus=15,  # parallelize distance computations
        attributes=attributes,  # list of entity attribute names
        path_database="./.dedupe/test.db",  # where to save the database holding intermediate data
        path_model="./.dedupe/test_model",  # where to save the model
        label_studio={
            "port": 8089,  # label studio port
            "api_key": "bc66ff77abeefc91a5fecd031fc0c238f9ad4814",  # label studio port
            "description": "gs test project",  # label studio description of project
        },
        fast_api={"port": 8003},  # fast api port
    ),
)
```

#### If saved:

a settings file is saved to deduper_settings_{name}.json

```
{
  "name": "test",
  "folder": ".dedupe",
  "other": {
    "attributes": [
      "givenname",
      "surname",
      "suburb",
      "postcode"
    ],
    "cpus": 15,
    "path_database": ".dedupe/test.db",
    "path_model": ".dedupe/test_model",
    "label_studio": {
      "host": "http://0.0.0.0",
      "port": 8089,
      "api_key": "bc66ff77abeefc91a5fecd031fc0c238f9ad4814",
      "description": "gs test project"
    },
    "fast_api": {
      "host": "http://0.0.0.0",
      "port": 8003
    }
  }
}
```