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

engine.execute("""
    CREATE FUNCTION exactmatch(s text) RETURNS text
    AS $$
    return s
    $$ 
    LANGUAGE plpython3u;
""")

engine.execute("""
    CREATE OR REPLACE FUNCTION unnest_2d_1d(ANYARRAY, OUT a ANYARRAY)
    RETURNS SETOF ANYARRAY
    LANGUAGE plpgsql IMMUTABLE STRICT AS
    $func$
    BEGIN
    FOREACH a SLICE 1 IN ARRAY $1 LOOP
        RETURN NEXT;
    END LOOP;
    END
    $func$;
""")


engine.execute("""
    CREATE FUNCTION combinations(arr integer[]) RETURNS integer[]
    AS $$
    def combinations(iterable, r):
        # combinations('ABCD', 2) --> AB AC AD BC BD CD
        # combinations(range(4), 3) --> 012 013 023 123
        pool = tuple(iterable)
        n = len(pool)
        if r > n:
            return
        indices = list(range(r))
        yield tuple(pool[i] for i in indices)
        while True:
            for i in reversed(range(r)):
                if indices[i] != i + n - r:
                    break
            else:
                return
            indices[i] += 1
            for j in range(i+1, r):
                indices[j] = indices[j-1] + 1
            yield tuple(pool[i] for i in indices)
    return [[s[0],s[1]] for s in combinations(arr,2)]
    $$ 
    LANGUAGE plpython3u;
""")




