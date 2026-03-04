#!/usr/bin/env python3
"""Generate Pydantic models from OpenAPI spec."""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OPENAPI_SPEC = PROJECT_ROOT / "openapi" / "product-api.yaml"
OUTPUT = PROJECT_ROOT / "generated" / "models.py"


def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "datamodel_code_generator",
            "--input",
            str(OPENAPI_SPEC),
            "--input-file-type",
            "openapi",
            "--output",
            str(OUTPUT),
            "--output-model-type",
            "pydantic_v2.BaseModel",
            "--target-python-version",
            "3.11",
        ],
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        sys.exit(result.returncode)
    print(f"Generated {OUTPUT}")


if __name__ == "__main__":
    main()
