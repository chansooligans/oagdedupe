# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})

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
import os
import seaborn as sns
from io import BytesIO
import base64
from werkzeug.utils import secure_filename

# %%
cache_path = "/home/csong/cs_github/deduper/cache"

# %%
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['UPLOAD_FOLDER'] = cache_path

app.init = Init(cache_path=cache_path)
app.init.setup_cache()
app.lab = utils.Labels(cache_path)

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        app.init.df = pd.read_csv(request.files.get('file'))
        app.init.setup_dedupe(app.init.df)
        app.init.d.trainer.labels = app.lab.labels
        f.save(f"{app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}")
    return redirect(url_for('active_learn'))
    

@app.route('/learn/', methods=["GET","POST"])
def active_learn():
    print(dir(app.init))
    if not hasattr(app.init, "d"):
        print(123)
        return render_template(
            'load.html', 
        )

    c_index = app.init.d.trainer.samples[app.lab._type][app.lab.sampleidx]
    idxl,idxr = app.init.idxmat[c_index]
    score = app.init.d.trainer.sorted_scores[app.lab._type][app.lab.sampleidx]
    sample1 = app.init.df.loc[idxl].to_dict()
    sample2 = app.init.df.loc[idxr].to_dict()

    img = BytesIO()
    # sns.kdeplot(app.init.d.trainer.sorted_scores["init"])
    fig = sns.scatterplot(x=0, y=1, hue = "scores", data=app.init.d.trainer.dfX)
    sns.set(rc={'figure.figsize':(5,4)})
    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    scatterplt = base64.b64encode(img.getvalue()).decode('utf8')

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
        scatterplt=scatterplt
    )

@app.route('/retrain', methods=["GET", "POST"])
def retrain():
    print(123)
    app.init.d.trainer.labels = app.lab.labels
    print(app.lab.labels)
    if request.method == "GET":
        app.init.d.trainer.retrain()
        app.init.d.trainer.get_samples()
        return "success"

app.run(host="pdcprlrdsci02",port=8008, debug=True)

# %%

