import configparser
import os
import logging

CONFIG_FOLDER_NAME = ".dedupe"

def get_dedupe_file_path(*filepath) -> str:
    home = os.path.expanduser("~")
    if home is None:
        raise RuntimeError("No home directory.")

    return os.path.join(home, CONFIG_FOLDER_NAME, *filepath)

def get_project_dedupe_file_path(*filepath):
    """Return the full path to a filepath in ${CWD}/.dedupe.
    """
    return os.path.join(os.getcwd(), CONFIG_FOLDER_NAME, *filepath)

CONFIG_FILENAMES = [
    get_dedupe_file_path("config.ini"),
    get_project_dedupe_file_path("config.ini"),
]

class Config:

    def __init__(self):
        
        self.config = configparser.ConfigParser()
        
        for filename in CONFIG_FILENAMES:
            if os.path.exists(filename):
                self.config.read(filename)
                break
        
        if self.config.sections() != ['MAIN', 'LABEL_STUDIO', 'FAST_API']:
            logging.error(
                """
                A config file is required, there are two options:

                (1) A global config file at ~/.dedupe/config.ini
                (2) A per-project config file at $CWD/.dedupe/config.ini, where $CWD is the folder you're running your script from (the folder where you `import dedupe`).

                The config file should contain:

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
                """
            )

    @property
    def host(self):
        return self.config["MAIN"]["HOST"]

    @property
    def ls_url(self):
        return f"""{self.host}:{self.config["LABEL_STUDIO"]["PORT"]}"""

    @property
    def ls_api_key(self):
        return self.config["LABEL_STUDIO"]["API_KEY"]

    @property
    def ls_title(self):
        return self.config["LABEL_STUDIO"]["TITLE"]

    @property
    def ls_description(self):
        return self.config["LABEL_STUDIO"]["DESCRIPTION"]

    @property
    def fast_api_port(self):
        return self.config["FAST_API"]["PORT"]
    
    @property
    def fast_api_url(self):
        return f"""{self.host}:{self.config["FAST_API"]["PORT"]}"""

    @property
    def model_fp(self):
        return self.config["MAIN"]["MODEL_FILEPATH"]

    @property
    def cache_fp(self):
        return self.config["MAIN"]["CACHE_FILEPATH"]


