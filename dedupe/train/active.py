from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property

import pandas as pd
import numpy as np
from modAL.models import ActiveLearner
from functools import partial
from modAL.batch import uncertainty_batch_sampling
from sklearn.ensemble import RandomForestClassifier

from superintendent import ClassLabeller
import pandas as pd
from IPython import display
import json
import time

import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})

from dedupe.base import BaseTrain

@dataclass
class ActiveJupyter(BaseTrain):
    """
    Model to implement active learning
    """

    def __post_init__(self):
        
        # Pre-set our batch sampling to retrieve 3 samples at a time.
        self.BATCH_SIZE = 5
        preset_batch = partial(uncertainty_batch_sampling, n_instances=self.BATCH_SIZE)

        # initializing the learner
        self.clf = ActiveLearner(
            estimator=RandomForestClassifier(),
            query_strategy=preset_batch,
        )


    def learn(self, df, X, idxmat):

        y = np.repeat(0,len(X))
        X_pool = X.copy()
        
        finished = False 
        while finished == False:
            
            # Pool-based sampling
            query_index, query_instance = self.clf.query(X_pool)

            learn_X = pd.concat([
                (
                    df
                    .loc[idxmat[query_index,0]]
                    .reset_index()
                    .set_axis(["id_l","name_l","addr_l"], axis=1)
                ),
                (
                    df
                    .loc[idxmat[query_index,1]]
                    .reset_index()
                    .set_axis(["id_r","name_r","addr_r"], axis=1)
                )
            ], axis=1).to_dict("records")
            
            i = 0
            n = len(learn_X)
            new_labels = []
            while i < n:
                
                print(json.dumps(
                    learn_X[i],
                    sort_keys=True,
                    indent=4
                ))

                time.sleep(1)

                user_label = str(
                    input("Enter 1 (match), 2 (non-match), 3 (skip), or enter 'exit'")
                ).lower()

                main_options = {
                    "1":1,
                    "2":0,
                    "3":"skip",
                }

                if user_label == "exit":
                    finished = True
                    break
                elif user_label in ["1", "2", "3"]:
                    user_label = main_options[user_label]
                    i+=1
                elif user_label == "p":
                    i = max(i-1,0)
                else:
                    user_label = "skip"
                    i+=1

                new_labels.append(user_label)

            lablled_idx = []
            labels = []
            for i,label in enumerate(new_labels):
                if label != "skip":
                    lablled_idx.append(query_index[i])
                    labels.append(label)

            self.clf.teach(X=X_pool[lablled_idx], y=labels)

            X_pool = np.delete(X_pool, query_index, axis=0)

            plt.figure()
            sns.scatterplot(X[:,0], X[:,1], self.clf.predict(X))
            plt.show()
        

    def fit(self, X):
        return self.clf.predict_proba(X),self.clf.predict(X)