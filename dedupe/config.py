import configparser
from dedupe import DIR

class Config:

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(f"{DIR}/.dedupe/.config")

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

