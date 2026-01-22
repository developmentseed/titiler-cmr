# check=skip=JSONArgsRecommended
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ARG EARTHDATA_USERNAME
ARG EARTHDATA_PASSWORD

# Check if EARTHDATA_USERNAME and EARTHDATA_PASSWORD are provided
RUN <<'EOF'
if [ -z "$EARTHDATA_USERNAME" ] || [ -z "$EARTHDATA_PASSWORD" ]; then
  echo "Error: EARTHDATA_USERNAME and EARTHDATA_PASSWORD build args must be provided"
  exit 1
fi

echo "machine urs.earthdata.nasa.gov login ${EARTHDATA_USERNAME} password ${EARTHDATA_PASSWORD}" > ~/.netrc
unset EARTHDATA_USERNAME EARTHDATA_PASSWORD
EOF

RUN <<'EOF'
apt-get update
apt-get -y --no-install-recommends install libexpat1
rm -rf /var/lib/apt/lists/*
EOF

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./
COPY titiler ./titiler

RUN uv sync --no-dev --frozen --extra uvicorn

# http://www.uvicorn.org/settings/
ENV HOST=0.0.0.0
ENV PORT=80
CMD uv run --no-dev uvicorn titiler.cmr.main:app --host ${HOST} --port ${PORT} --log-level debug --reload
