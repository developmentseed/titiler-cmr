"""Tests for the Lambda handler."""

import os
import subprocess
import sys
from pathlib import Path

import rasterio


def test_handler_imports_with_telemetry_enabled():
    """The Lambda handler imports with telemetry enabled.

    The handler sets up telemetry when it is imported, so a failure there stops
    the Lambda from starting. The import runs in a separate process because it
    instruments the app and replaces the global tracer provider, which would
    leak into other tests. No spans are exported, so the X-Ray connection is
    not covered.
    """
    rasterio_dir = Path(rasterio.__file__).parent
    env = {
        **os.environ,
        # The handler enables telemetry only when this variable shows it runs
        # on Lambda.
        "AWS_EXECUTION_ENV": "AWS_Lambda_python3.12",
        "TITILER_CMR_TELEMETRY_ENABLED": "true",
        # Replace the handler's Lambda paths with rasterio's bundled data.
        "GDAL_DATA": str(rasterio_dir / "gdal_data"),
        "PROJ_DATA": str(rasterio_dir / "proj_data"),
    }

    result = subprocess.run(
        [sys.executable, "-c", "import handler"],
        cwd=Path(__file__).parents[1] / "infrastructure" / "aws" / "lambda",
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
