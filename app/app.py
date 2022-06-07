# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')

from dedupe.app import utils
from dedupe.app.init import Init
from flask import Flask
from flask import (
    render_template, 
    request,
    redirect,
    url_for,
)
import pandas as pd
import glob
import seaborn as sns
from werkzeug.utils import secure_filename

# %%
cache_path = "/home/csong/cs_github/deduper/cache"

# %%
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['UPLOAD_FOLDER'] = cache_path
app.cached_files = [
    x
    for x in glob.glob(f"{app.config['UPLOAD_FOLDER']}/*.csv")
]

app.init = Init(cache_path=cache_path)
app.lab = utils.Labels(cache_path)
app.init.setup_cache()

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    print(request.form)
    if request.method == 'POST':
        f = request.files['file']
        app.init._load_default_dataset(request.files.get('file'), app.lab.labels)
        f.save(f"{app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}")
    return redirect(url_for('active_learn'))
    
@app.route('/load', methods=["GET","POST"])
def load_page():
    return render_template(
        'load.html', 
        entries=app.cached_files
    )

@app.route('/plots', methods=["GET","POST"])
def load_plots():

    if not hasattr(app.init, "d"):
        app.init._load_default_dataset(app.cached_files[0], app.lab.labels)

    scatterplt, kdeplot = utils.get_plots(app.init.trainer.dfX)
    return render_template(
        'plots.html', 
        scatterplt=scatterplt,
        kdeplot=kdeplot
    )

@app.route('/learn', methods=["GET","POST"])
def active_learn():

    if not hasattr(app.init, "d"):
        app.init._load_default_dataset(app.cached_files[0], app.lab.labels)

    c_index = app.init.d.trainer.samples[app.lab._type][app.lab.sampleidx]
    idxl,idxr = app.init.idxmat[c_index]
    score = app.init.d.trainer.sorted_scores[app.lab._type][app.lab.sampleidx]
    sample1 = app.init.df.loc[idxl].to_dict()
    sample2 = app.init.df.loc[idxr].to_dict()

    if request.method == "POST":
        app.lab.labels[app.lab.sampleidx] = {
            "ids":f"{idxl}|{idxr}",
            "c_index":f"{c_index}",
            "score":f"{score}",
            "label":request.form["btnradio"]
        }
        app.lab.meta[request.form["btnradio"]] += 1
        app.lab.meta[app.lab._type] += 1
        app.lab.meta[app.lab._type+"_current"] += 1
        app.lab.save()
        return redirect(url_for('active_learn'))

    return render_template(
        'learn.html', 
        sample1=sample1,
        sample2=sample2,
        score=score,
        labels=app.lab.labels,
        meta=app.lab.meta,
    )

@app.route('/retrain', methods=["GET", "POST"])
def retrain():
    app.init.d.trainer.labels = app.lab.labels
    if request.method == "GET":
        app.init.d.trainer.retrain()
        app.init.d.trainer.get_samples()
        app.lab.meta[app.lab._type+"_current"] = 0
        return "success"

app.run(host="pdcprlrdsci02",port=8008, debug=True)

# %%

