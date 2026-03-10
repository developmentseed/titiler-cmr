FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN <<'EOF'
apt-get update
apt-get -y --no-install-recommends install libexpat1 git
rm -rf /var/lib/apt/lists/*
EOF

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./
RUN <<EOF
uv sync --no-dev --frozen --extra uvicorn
# Remove *.dist-info directories
find .venv/lib/python3.12/site-packages -mindepth 1 -maxdepth 1 -type d -name '*.dist-info' -exec rm -rf {} \;
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
