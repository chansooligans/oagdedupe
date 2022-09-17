# %%
from sqlalchemy import create_engine
engine = create_engine(f"postgresql+psycopg2://username:password@172.22.39.26:8000/db", echo=False)

engine.execute("""
    CREATE FUNCTION first_nchars(s text, n integer) RETURNS text
    AS $$
    return s[:n]
    $$ 
    LANGUAGE plpython3u;
""")

engine.execute("""
    CREATE FUNCTION last_nchars(s text, n integer) RETURNS text
    AS $$
    return s[-n:]
    $$ 
    LANGUAGE plpython3u;
""")

engine.execute("""
    CREATE FUNCTION find_ngrams(s text, n integer) RETURNS text[]
    AS $$
    return [s[i:i+n] for i in range(len(s)-n+1)]
    $$ 
    LANGUAGE plpython3u;
""")

engine.execute("""
    CREATE FUNCTION acronym(s text) RETURNS text
    AS $$
    return "".join(e[0] for e in s.split())
    $$ 
    LANGUAGE plpython3u;
""")



