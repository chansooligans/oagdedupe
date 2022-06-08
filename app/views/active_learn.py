from .. import app

import pandas as pd
from flask import (
    render_template, 
    request,
    redirect,
    url_for,
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