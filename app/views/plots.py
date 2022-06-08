from .. import app

from . import utils

from flask import (
    render_template, 
    request,
    redirect,
    url_for,   
)
from werkzeug.utils import secure_filename

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
