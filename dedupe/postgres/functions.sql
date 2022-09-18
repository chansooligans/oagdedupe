create language pg_trgm;
create language plpython3u;
CREATE FUNCTION first_nchars(s text, n integer) RETURNS text
AS $$
return s[:n]
$$ 
LANGUAGE plpython3u;