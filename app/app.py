import streamlit as st
st.set_page_config(layout="wide")
from st_aggrid import AgGrid
import pandas as pd
from glob import glob

df = pd.read_csv('cache/test.csv')
grid_return = AgGrid(
    df, 
    editable=True,
    fit_columns_on_grid_load=True
)

if st.button('Save'):
    grid_return['data'].to_csv('cache/test.csv', index=False)


# streamlit run app/app.py --server.port 8089
