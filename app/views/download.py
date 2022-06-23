from .. import app
from .. import utils

from flask import (
    render_template, 
    request,   
    session,
    send_file,
    redirect
)

import io

@app.route('/results', methods=["GET", "POST"])
def results():

    if not hasattr(app.init, "d"):
        app.logger.error("need to load dataset first")
        return redirect('/load/nodata')

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
