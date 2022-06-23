from .. import app
from .. import utils

from flask import (
    render_template, 
    request,
    redirect,
    url_for,
)


def get_samples(samples, idxmat, labelled):
    """
    retrieves index of idxmat and its contents (idxl, idxr)

    samples:
        Deduper().trainer.samples; 
        Scores represent 
        Contains indices of idxmat, categorized by "low", "high" and "uncertain"
            - "low": indices with lowest probability of match
            - "high": indices with highest probability of match
            - "uncertain": indices with smallest absolute difference from 0.5

    idxmat:
        array of pairs of record IDs where each pair is a candidate for comparison

    labelled:
        already labelled indices of idxmat
    """
    selected_idx = samples[0]
    if selected_idx in labelled:
        app.logger.info(f"{selected_idx} already labelled; proceeding to next candidate")
        selected_idx = samples[1]
        samples.popleft()
    idxl, idxr = idxmat[selected_idx]
    return selected_idx, idxl, idxr


def add_labelled_sample(lab, idxl, idxr, selected_idx, score):
    """
    saves newly labelled sample

    lab: 
        labelled samples manager object ("app.lab")
    idxl:
        index of record A
    idxlr:
        index of record B
    selected_idx:
        index of comparison in idxmat
    score:
        probability of match
    """
    lab.labels[f"{idxl}-{idxr}"] = {
        "idl": f"{idxl}",
        "idr": f"{idxr}",
        "idxmat_idx": f"{selected_idx}",
        "score": score,
        "label": request.form["btnradio"],
        "type": lab._type,
        "revise": f"<a href=/learn/{idxl}-{idxr}>edit</a>"
    }
    if f"{idxl}-{idxr}" not in lab.labels.keys():
        lab.meta[request.form["btnradio"]] += 1
        lab.meta[lab._type] += 1
    lab.save()
    return redirect(url_for('learn'))


@app.route('/learn', methods=["GET", "POST"])
@app.route('/learn/<int:idxl>-<int:idxr>', methods=["GET", "POST"])
def learn(idxl=None, idxr=None):
    """
    loads the "/learn" page

    when form is submitted with an option ("yes", "no", "uncertain"); 
    submits POST request to update samples json in cache

    if user is directed to "/learn/<int:idxl>-<int:idxr>", then loads 
    comparison of entities with ID idxl and idxr
    """

    if not hasattr(app.init, "d"):
        app.logger.error("need to load dataset first")
        return redirect('/load/nodata')

    if idxl is None:
        selected_idx, idxl, idxr = get_samples(
            samples=app.init.d.trainer.samples[app.lab._type], 
            idxmat=app.init.idxmat, 
            labelled=app.lab.labelled
        )
    else:
        selected_idx = app.init.idxmat.tolist().index([idxl, idxr])

    score = round(app.init.d.trainer.scores[selected_idx], 4)
    df = utils.labels_to_df(app.lab.labels)

    if request.method == "POST":
        add_labelled_sample(
            lab=app.lab,
            idxl=idxl,
            idxr=idxr,
            selected_idx=selected_idx,
            score=score
        )

    return render_template(
        'learn.html', 
        dataset_filename=app.init.datset_filename,
        sample1=app.init.df.loc[idxl].to_dict(),
        sample2=app.init.df.loc[idxr].to_dict(),
        score=score,
        label_df=df.to_html(index=False, escape=False),
        meta=app.lab.meta,
    )


@app.route('/retrain', methods=["GET"])
def retrain():
    """
    if user submits retrain option, re-trains the model with latest labelled samples

    if there are inadequate # of samples (e.g. all have same class), then 
    re-initializes model (train using full data with random assignment)
    """
    app.logger.info("retraining")
    app.init.d.trainer.labels = app.lab.labels
    if request.method == "GET":
        try:
            app.init.d.trainer.retrain()
            return "success"
        except Exception as e:
            app.logger.error(e)
            app.init.d.trainer.initialize(app.init.X)
            return "re-initialize"
