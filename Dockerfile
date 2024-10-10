ARG PYTHON_VERSION=3.11
ARG EARTHDATA_USERNAME
ARG EARTHDATA_PASSWORD

FROM python:${PYTHON_VERSION}-slim

WORKDIR /tmp

COPY titiler/ titiler/
COPY pyproject.toml pyproject.toml
COPY README.md README.md
COPY LICENSE LICENSE

RUN pip install --no-cache-dir --upgrade . uvicorn
RUN rm -rf titiler/ pyproject.toml README.md LICENSE
RUN echo -e "machine urs.earthdata.nasa.gov\nlogin ${EARTHDATA_USERNAME}\npassword ${EARTHDATA_PASSWORD}" > ~/.netrc && \
  unset EARTHDATA_USERNAME && \
  unset EARTHDATA_PASSWORD

# http://www.uvicorn.org/settings/
ENV HOST 0.0.0.0
ENV PORT 80
CMD uvicorn titiler.cmr.main:app --host ${HOST} --port ${PORT} --log-level debug

