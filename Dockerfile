FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN <<'EOF'
apt-get update
apt-get -y --no-install-recommends install libexpat1
rm -rf /var/lib/apt/lists/*
EOF

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --no-dev --frozen --extra uvicorn

COPY titiler ./titiler

RUN \
  --mount=type=secret,id=earthdata-username,required,env=EARTHDATA_USERNAME \
  --mount=type=secret,id=earthdata-password,required,env=EARTHDATA_PASSWORD \
  echo "machine urs.earthdata.nasa.gov login ${EARTHDATA_USERNAME:?} password ${EARTHDATA_PASSWORD:?}" > ~/.netrc

# http://www.uvicorn.org/settings/
ENV HOST=0.0.0.0
ENV PORT=80
CMD ["/bin/sh", "-c", "uv run --no-dev uvicorn titiler.cmr.main:app --host ${HOST} --port ${PORT} --log-level debug --reload"]
