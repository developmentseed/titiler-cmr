FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock

COPY titiler/ titiler/
COPY README.md README.md
COPY LICENSE LICENSE

RUN apt-get update && apt-get -y --no-install-recommends install libexpat1 && uv sync --frozen --extra uvicorn

ARG EARTHDATA_USERNAME
ARG EARTHDATA_PASSWORD

# Check if EARTHDATA_USERNAME and EARTHDATA_PASSWORD are provided
RUN if [ -z "$EARTHDATA_USERNAME" ] || [ -z "$EARTHDATA_PASSWORD" ]; then \
  echo "Error: EARTHDATA_USERNAME and EARTHDATA_PASSWORD build args must be provided"; \
  exit 1; \
  fi && \
  echo "machine urs.earthdata.nasa.gov\nlogin ${EARTHDATA_USERNAME}\npassword ${EARTHDATA_PASSWORD}" > ~/.netrc && \
  unset EARTHDATA_USERNAME && \
  unset EARTHDATA_PASSWORD

# http://www.uvicorn.org/settings/
ENV HOST 0.0.0.0
ENV PORT 80
CMD uv run uvicorn titiler.cmr.main:app --host ${HOST} --port ${PORT} --log-level debug --reload

