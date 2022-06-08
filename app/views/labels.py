from .. import app

import pandas as pd
import os
from flask import (
    render_template, 
    request,
    redirect
)

@app.route('/labels', methods=["GET","POST"])
def load_labels(idxl=None,idxr=None):

    if not hasattr(app.init, "d"):
        return redirect('/load/nodata')

    if len(app.lab.labels) > 0:
        df = pd.DataFrame(app.lab.labels).T[[
            "idl", "idr", "label", "revise"
        ]]
    else:
        df = pd.DataFrame(app.lab.labels).T

    return render_template(
        'labels.html', 
        label_df=df.to_html(index=False, escape=False),
        meta=app.lab.meta,
    )

@app.route('/reset', methods=["GET", "POST"])
def reset():
    print("resetting")
    if request.method == "GET":
        os.remove(f"{app.config['UPLOAD_FOLDER']}/samples.json")
        os.remove(f"{app.config['UPLOAD_FOLDER']}/meta.json")
        app.init.setup_cache()
        del app.lab.labels
        return "reset"