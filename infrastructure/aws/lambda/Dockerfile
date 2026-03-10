ARG PYTHON_VERSION=3.12

# Stage 1: OTEL
# Download the OpenTelemetry layer
# ref: https://github.com/athewsey/opentelemetry-lambda-container/blob/98069d5eb6d812ccd28d5c80e2f9d6c8a8c76fb9/python-example/lambda-function/Dockerfile
FROM public.ecr.aws/lambda/python:${PYTHON_VERSION} AS otel-builder

RUN <<EOF
dnf install -y unzip wget
dnf clean all
wget -q https://github.com/aws-observability/aws-otel-python-instrumentation/releases/download/v0.12.1/layer.zip -O /tmp/layer.zip
mkdir -p /opt-builder
unzip -q /tmp/layer.zip -d /opt-builder/
EOF

# Stage 2: titiler-cmr application and dependencies
FROM public.ecr.aws/lambda/python:${PYTHON_VERSION} AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN dnf install -y gcc-c++ git && dnf clean all

WORKDIR /build

COPY README.md uv.lock .python-version pyproject.toml ./
COPY titiler/ ./titiler/

RUN <<EOF
uv export --quiet --locked --no-editable --no-dev --extra lambda --extra telemetry --format requirements.txt -o requirements.txt
uv pip install \
  --compile-bytecode \
  --no-binary pydantic \
  --target /deps \
  --no-cache-dir \
  -r requirements.txt
EOF

# Aggressive cleanup to minimize size and optimize for Lambda container
# Clean up app dependencies in /deps
WORKDIR /deps
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN <<EOF
# Convert .pyc files and remove source .py files for faster cold starts
find . -type f -name '*.pyc' | while read -r f; do n="$(echo "$f" | sed 's/__pycache__\///' | sed 's/.cpython-[0-9]*//')"; cp "$f" "$n"; done
find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
find . -type f -a -name '*.py' -print0 | xargs -0 rm -f
# Remove unnecessary files for Lambda runtime
find . -type d -a -name 'tests' -print0 | xargs -0 rm -rf
find . -type d -a -name 'test' -print0 | xargs -0 rm -rf
rm -rf numpy/doc/ bin/ geos_license Misc/
# Remove unnecessary locale and documentation files
find . -name '*.mo' -delete
find . -name '*.po' -delete
find . -name 'LICENSE*' -print0 | xargs -0 rm -rf
find . -name 'README*' -delete
find . -name '*.md' -delete
# Strip debug symbols from shared libraries (preserve numpy.libs)
find . -type f -name '*.so*' -not -path "*/numpy.libs/*" -exec strip --strip-unneeded {} \; 2>/dev/null || true
# Remove *.dist-info directories
find /deps -mindepth 1 -maxdepth 1 -type d -name '*.dist-info' -exec rm -rf {} \;
EOF


# Stage 3: Final runtime stage - minimal Lambda image optimized for container runtime
FROM public.ecr.aws/lambda/python:${PYTHON_VERSION}

ENV PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1

COPY --from=otel-builder /opt-builder/ /opt/
COPY infrastructure/aws/lambda/collector-config.yaml /opt/collector-config/config.yaml
COPY --from=builder /deps ${LAMBDA_RUNTIME_DIR}/
COPY --from=builder /usr/lib64/libexpat.so.1 ${LAMBDA_RUNTIME_DIR}/
COPY infrastructure/aws/lambda/handler.py ${LAMBDA_RUNTIME_DIR}/

RUN <<EOF
chmod 644 "${LAMBDA_RUNTIME_DIR}"/handler.py
chmod -R 755 /opt/
# Pre-compile the handler for faster cold starts
python -c "import py_compile; py_compile.compile('${LAMBDA_RUNTIME_DIR}/handler.py', doraise=True)"
# Create cache directories with proper permissions
mkdir -p /tmp/.cache && chmod 777 /tmp/.cache
EOF

CMD ["handler.lambda_handler"]
