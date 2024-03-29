# %%
from diagrams import Diagram, Edge, Cluster
from diagrams.custom import Custom
from diagrams.generic.database import SQL
from diagrams.programming.language import Python
from diagrams.generic.storage import Storage


with Diagram("Dedupe", show=False):
    rawdata = Storage("Raw Data (local)")
    preprocess = Python("PreProcess")
    
    rawdata >> preprocess 

    with Cluster("Active Learning Loop"):
        sql = SQL("Postgres db")
        model = Custom("Model", "img/modAl.png") 
        preprocess >> sql 

        fastapi = Custom("FastApi","img/fastapi.png")
        labelstudio = Custom("LabelStudio","img/labelstudio.png")
        model >> Edge(color="darkgreen") << fastapi
        sql >> Edge(color="darkgreen") << fastapi
        fastapi >> Edge(color="darkgreen") <<  labelstudio
    
    networkX = Custom("NetworkX", "img/networkX.png")
    fastapi >> networkX >> Storage("Predictions (local)")
    
# %%
