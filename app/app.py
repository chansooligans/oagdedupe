# %%
from IPython import get_ipython
if get_ipython() is not None:
    get_ipython().run_line_magic('load_ext', 'autoreload')
    get_ipython().run_line_magic('autoreload', '2')
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})

from dedupe.app import (
    init,
    utils
)
from flask import Flask
from flask import (
    render_template, 
    request,
    redirect,
    url_for,
)
import json

# %%
cache_path = "/home/csong/cs_github/deduper/cache"
init.setup_cache(cache_path)
from dedupe.datasets.fake import df, df2
d, idxmat = init.setup_dedupe(df)
lab = utils.Labels(cache_path)
d.trainer.labels = lab.labels


# %%
app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

@app.route('/learn/', methods=["GET","POST"])
def active_learn():

    c_index = d.trainer.samples[lab._type][lab.sampleidx]
    idxl,idxr = idxmat[c_index]
    score = d.trainer.sorted_scores[lab._type][lab.sampleidx]
    sample1 = df.loc[idxl].to_dict()
    sample2 = df.loc[idxr].to_dict()

    if request.method == "POST":
        lab.labels[lab.sampleidx] = {
            "ids":f"{idxl}|{idxr}",
            "c_index":f"{c_index}",
            "score":f"{score}",
            "label":request.form["btnradio"]
        }
        lab.meta[request.form["btnradio"]] += 1
        lab.meta[lab._type] += 1
        lab.meta[lab._type+"_current"] += 1
        lab.save()
        return redirect(url_for('active_learn'))

    return render_template(
        'learn.html', 
        sample1=sample1,
        sample2=sample2,
        score=score,
        labels=lab.labels,
        meta=lab.meta,
    )

@app.route('/retrain', methods=["GET", "POST"])
def retrain():
    print(123)
    d.trainer.labels = lab.labels
    print(lab.labels)
    if request.method == "GET":
        d.trainer.retrain()
        return redirect(url_for('active_learn'))

app.run(host="pdcprlrdsci02",port=8008, debug=True)

# %%

# %%


# # %%
# samples["label"] = [0,0,0,0,1,1,1,1,1,1]

# d.trainer.labels[_type] = samples["label"].values

# for idx,lab in zip(d.trainer.samples[_type], d.trainer.labels[_type]):
#     d.trainer.active_dict[idx] = lab

# d.trainer.train(
#     X[list(d.trainer.active_dict.keys()),:], 
#     init=False, 
#     labels=list(d.trainer.active_dict.values())
# )
# d.trainer.scores, d.trainer.y = d.trainer.fit(X)


# # %%
# _type = "uncertain"

# samples = pd.concat(
#     [
#         df.loc[idxmat[d.trainer.samples[_type],0]].reset_index(drop=True),
#         df.loc[idxmat[d.trainer.samples[_type],1]].reset_index(drop=True)
#     ],
#     axis=1
# ).assign(
#     score=d.trainer.scores[d.trainer.samples[_type]],
#     label=None
# )
# samples

# # %%
# samples["label"] = [1,1,1,1,1]

# d.trainer.labels[_type] = samples["label"].values

# for idx,lab in zip(d.trainer.samples[_type], d.trainer.labels[_type]):
#     d.trainer.active_dict[idx] = lab

# d.trainer.train(
#     X[list(d.trainer.active_dict.keys()),:], 
#     init=False, 
#     labels=list(d.trainer.active_dict.values())
# )
# d.trainer.scores, d.trainer.y = d.trainer.fit(X)



# %%
