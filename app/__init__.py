# %%
from . import utils
from .init import Init
# from .views import (
#     active_learn,
#     load_page,
#     plots,
#     download
# )

from flask import Flask
from flask import (
    render_template, 
    request,
    redirect,
    url_for,
    send_file,
    session
)
import pandas as pd
import glob
import io
from werkzeug.utils import secure_filename

cache_path = "/home/csong/cs_github/deduper/cache"

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8008, debug=True)
# %%
