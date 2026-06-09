#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CDK_DIR="${ROOT_DIR}/infrastructure/aws"
OUT_DIR="${CDK_OUTDIR:-${CDK_DIR}/cdk.out.test}"

export TITILER_CMR_EARTHDATA_USERNAME="${TITILER_CMR_EARTHDATA_USERNAME:-placeholder-username}"
export TITILER_CMR_EARTHDATA_PASSWORD="${TITILER_CMR_EARTHDATA_PASSWORD:-placeholder-password}"
export TITILER_CMR_EARTHDATA_S3_DIRECT_ACCESS="${TITILER_CMR_EARTHDATA_S3_DIRECT_ACCESS:-TRUE}"
export TITILER_CMR_ROOT_PATH="${TITILER_CMR_ROOT_PATH:-}"
export STAGE="${STAGE:-test}"

cd "${CDK_DIR}"

uv sync --group deployment --inexact
npm ci
rm -rf "${OUT_DIR}"

printf 'Synthesizing CDK app with placeholder environment into %s\n' "${OUT_DIR}"
uv run --group deployment npm run cdk -- synth --output "${OUT_DIR}"
