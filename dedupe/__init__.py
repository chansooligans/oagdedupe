import os
# DIR = os.path.dirname(os.path.abspath(__file__)).rsplit("/", 1)[0]

DIR = os.path.expanduser("~")
print(DIR)

from dedupe.config import Config
config = Config()