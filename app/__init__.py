from . import utils
from .init import Init
from flask import Flask
import glob

cache_path = "cache"

app = Flask(__name__)
app.secret_key = b"_j'yXdW7.63}}b7"
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['UPLOAD_FOLDER'] = cache_path
app.cached_files = [
    x
    for x in glob.glob(f"{app.config['UPLOAD_FOLDER']}/*.csv")
]
app.init = Init(cache_path=cache_path)
app.lab = utils.Labels(cache_path)
app.init.setup_cache()

from .views import (
    load_page,
    plots,
    download,
    active_learn,
    labels,
)