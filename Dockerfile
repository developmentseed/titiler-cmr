ARG APP_VERSION=0.0.0
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN <<'EOF'
apt-get update
apt-get -y --no-install-recommends install libexpat1 git
rm -rf /var/lib/apt/lists/*
EOF

WORKDIR /app

ARG APP_VERSION
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN <<EOF
UV_DYNAMIC_VERSIONING_BYPASS=$APP_VERSION uv sync --no-dev --frozen --extra uvicorn
# Remove *.dist-info directories except titiler_cmr (needed for importlib.metadata version lookup)
find .venv/lib/python3.12/site-packages -mindepth 1 -maxdepth 1 -type d -name '*.dist-info' ! -name 'titiler_cmr*' -exec rm -rf {} \;
EOF

COPY titiler ./titiler

RUN \
  --mount=type=secret,id=earthdata-username,required,env=EARTHDATA_USERNAME \
  --mount=type=secret,id=earthdata-password,required,env=EARTHDATA_PASSWORD \
  echo "machine urs.earthdata.nasa.gov login ${EARTHDATA_USERNAME:?} password ${EARTHDATA_PASSWORD:?}" > ~/.netrc

# http://www.uvicorn.org/settings/
ENV HOST=0.0.0.0
ENV PORT=80
CMD ["/bin/sh", "-c", "uv run --no-dev uvicorn titiler.cmr.main:app --host ${HOST} --port ${PORT} --log-level debug --reload"]
