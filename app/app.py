# %%
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})
import streamlit as st
import streamlit.components.v1 as components

def run(self,candidates, df ,_type="lowhigh"):
    self.get_samples()
    if _type == "lowhigh":
        for x in ["lowest","highest"]:
            self.active_learn(x, candidates, df)
    else:
        self.active_learn("uncertain", candidates, df)
    self.update_active_dict(_type)
    self.retrain()
    plt.figure()
    sns.scatterplot(self.X[:,0],self.X[:,1], hue=self.scores)
    plt.figure()
    sns.scatterplot(self.X[:,0],self.X[:,1], hue=self.y)
    plt.show()

def active_learn(self, _type, candidates, df):
    self.labels[_type] = []
    for a,b in candidates[self.samples[_type],:]:
        while True:
            try:
                userinput = int(input(f"{df.loc[a]} \n\n {df.loc[b]}"))
            except ValueError:
                print("Needs to be 1 or 0")
                continue
            else:
                break
        self.labels[_type].append(userinput)

def update_active_dict(self,_type):
    if _type == "uncertain":
        indices = list(self.samples["uncertain"])
        labels = self.labels["uncertain"] 
    else:
        indices = list(self.samples["lowest"]) + list(self.samples["highest"])
        labels = self.labels["lowest"] + self.labels["highest"]
    for idx,lab in zip(indices, labels):
        self.active_dict[idx] = lab