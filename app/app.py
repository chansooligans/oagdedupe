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
        app.init._load_dataset(request.files.get('file'), app.lab)
        app.init.df.to_csv(f"{app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}", index=False)
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
        app.init._load_dataset(app.cached_files[0], app.lab)

    scatterplt, kdeplot = utils.get_plots(app.init.d.trainer.dfX)
    return render_template(
        'plots.html', 
        scatterplt=scatterplt,
        kdeplot=kdeplot
    )

@app.route('/learn', methods=["GET","POST"])
@app.route('/learn/<int:idxl>-<int:idxr>', methods=["GET","POST"])
def active_learn(idxl=None,idxr=None):

    if not hasattr(app.init, "d"):
        app.init._load_dataset(app.cached_files[0], app.lab)

    if idxl is None or idxr is None:
        selected_idx = app.init.d.trainer.samples[app.lab._type].popleft()
        idxl,idxr = app.init.idxmat[selected_idx]
    else:
        selected_idx = app.init.idxmat.tolist().index([idxl,idxr])

    score = app.init.d.trainer.scores[selected_idx]
    sample1 = app.init.df.loc[idxl].to_dict()
    sample2 = app.init.df.loc[idxr].to_dict()

    if len(app.lab.labels) > 0:
        df = pd.DataFrame(app.lab.labels).T[[
            "idl", "idr", "label", "revise"
        ]]
    else:
        df = pd.DataFrame(app.lab.labels).T
    
    if request.method == "POST":
        app.lab.labels[f"{idxl}-{idxr}"] = {
            "idl":f"{idxl}",
            "idr":f"{idxr}",
            "idxmat_idx":f"{selected_idx}",
            "score":f"{round(score,4)}",
            "label":request.form["btnradio"],
            "type":app.lab._type,
            "revise":f"<a href=/learn/{idxl}-{idxr}>edit</a>"
        }
        if f"{idxl}-{idxr}" not in app.lab.labels.keys():
            app.lab.meta[request.form["btnradio"]] += 1
            app.lab.meta[app.lab._type] += 1
        app.lab.save()
        return redirect(url_for('active_learn'))

    return render_template(
        'learn.html', 
        dataset_filename=app.init.datset_filename,
        sample1=sample1,
        sample2=sample2,
        score=round(score,4),
        label_df=df.to_html(index=False, escape=False),
        meta=app.lab.meta,
    )

@app.route('/retrain', methods=["GET", "POST"])
def retrain():
    print(123)
    app.init.d.trainer.labels = app.lab.labels
    if request.method == "GET":
        app.init.d.trainer.retrain()
        return "success"

app.run(host="pdcprlrdsci02",port=8008, debug=True)


# %%

# %%
