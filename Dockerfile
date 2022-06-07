FROM python:3.8-slim-buster
WORKDIR /app

ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.1.11 \
    YOUR_ENV=development

# System deps:
RUN apt-get -qq update && \
apt-get install -qy --no-install-recommends \
   curl 

RUN apt-get install -qy --no-install-recommends \
    make 

RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
ENV PATH "/root/.local/bin:$PATH"

# install dependencies
COPY pyproject.toml poetry.lock ./
COPY . .
RUN poetry install $(test "$YOUR_ENV" == production && echo "--no-dev") --no-interaction --no-ansi

CMD ["make", "serve"]