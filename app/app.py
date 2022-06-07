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
    send_file,
    session
)
import pandas as pd
import glob
import io
from werkzeug.utils import secure_filename

# %%
cache_path = "/home/csong/cs_github/deduper/cache"

# %%
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

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    print("uploading")
    if request.method == 'POST':
        if request.files['file']:
            f = request.files['file']
            app.init._load_dataset(request.files.get('file'), app.lab)
            app.init.df.to_csv(f"{app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}", index=False)
        else:
            f = request.form.get('dataset-hidden-selection')
            app.init._load_dataset(request.form.get('dataset-hidden-selection'), app.lab)

    return redirect(url_for('active_learn'))

@app.route('/', methods=["GET","POST"])    
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

    scatterplt, kdeplot = utils.get_plots(
        X=app.init.d.trainer.X,
        scores=app.init.d.trainer.scores,
        attributes=app.init.d.attributes
    )
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
    print("retraining")
    app.init.d.trainer.labels = app.lab.labels
    if request.method == "GET":
        app.init.d.trainer.retrain()
        return "success"

@app.route('/results', methods=["GET", "POST"])
def results():

    return render_template(
        'results.html'
    )

@app.route('/download', methods=["GET"]) 
def download_csv():

    if request.method == "GET":

        scores, y = app.init.d.trainer.fit(
            app.init.d.trainer.X
        )
        df_clusters = app.init.d.cluster.get_df_cluster(
            matches=app.init.idxmat[y=="Yes"].astype(int), 
            scores=scores[y=="No"],
            rl=False
        )
        df_final = app.init.df.merge(
            df_clusters,
            left_index = True,
            right_on = "id"
        ).sort_values('cluster')

        session["df"] = df_final.to_csv(index=False, header=True)

        buf_str = io.StringIO(session["df"])
        buf_byt = io.BytesIO(buf_str.read().encode("utf-8"))

        return send_file(
            buf_byt,
            mimetype='text/csv',
            as_attachment=True,
            attachment_filename='results.csv',
        )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)