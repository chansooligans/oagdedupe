from .. import app
from .. import utils

from flask import (
    render_template,
    redirect
)

@app.route('/plots', methods=["GET","POST"])
def load_plots():

    if not hasattr(app.init, "d"):
        app.logger.error("need to load dataset first")
        return redirect('/load/nodata')

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
