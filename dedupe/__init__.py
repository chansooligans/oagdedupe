import os
DIR = os.path.dirname(os.path.abspath(__file__)).rsplit("/", 1)[0]

from dedupe.config import Config
config = Config()