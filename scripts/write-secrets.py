"""Initialize Docker secrets with Earthdata credentials for local development.

Run this once prior to running "docker compose up --build".  There is no need
to run it again unless your Earthdata credentials change.

Creates the following secret files within the ./secrets directory (git-ignored)
for use with Docker:

- earthdata-username.txt
- earthdata-password.txt

If necessary, creates the ./secrets directory with permissions 700, and creates
files within it with permissions 600.

Attempts to populate each secret as follows, in descending order of precedence:

1. Looks for corresponding environment variable (EARTHDATA_USERNAME and
   EARTHDATA_PASSWORD).
2. Loads from .env.secrets file, if exists.
3. Loads from .env file, if exists.
4. Loads from netrc file, if exists (defaults to ~/.netrc, but can set NETRC env var)
5. Prompts user as last resort.

This will ALWAYS overwrite existing secret files.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "python-dotenv>=1.2.1",
# ]
# ///
import logging
import netrc
import os
from getpass import getpass
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values

logging.basicConfig(level=logging.INFO)


def read_env() -> Mapping[str, str]:
    """Read environment variables, env files, and netrc as unified 'environment'.

    Returns a mapping of "environment variables", obtained from the following
    sources in descending order of precedence:

    - Environment variables (highest precedence)
    - Variables from .env.secrets, if it exists
    - Variables from .env, if it exists
    - netrc file, if it exists (default location can be overridden via NETRC
      environment variable): if an entry is found for host urs.earthdata.nasa.gov,
      sets default values on returned mapping for EARTHDATA_USERNAME and
      EARTHDATA_PASSWORD (i.e., only visible if these are not already set by
      a source listed above, with higher precedence)
    """
    env = {
        **dotenv_values(".env"),
        **dotenv_values(".env.secrets"),
        **os.environ,
    }

    if auth := netrc.netrc(env.get("NETRC")).authenticators("urs.earthdata.nasa.gov"):
        username, _, password = auth
        env.setdefault("EARTHDATA_USERNAME", username)
        env.setdefault("EARTHDATA_PASSWORD", password)

    return env


def ensure_secrets_dir() -> Path:
    """Make sure the secrets directory exists with mode 0o700 (rwx for user only)."""
    path = Path("./secrets")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)

    return path


def to_kebab_case(name: str) -> str:
    """Convert variable name to kebab case.

    Example: EARTHDATA_USERNAME becomes earthdata-username
    """
    return name.lower().replace("_", "-")


def ask_user(prompt: str, sensitive: bool) -> str:
    """Prompt user for a possibly sensitive value."""
    return getpass(prompt) if sensitive else input(prompt)


def write_secret(secrets_dir: Path, name: str, value: str) -> None:
    """Write a secret (with only rw perms for user) to the secrets directory."""
    file = (secrets_dir / name).with_suffix(".txt")
    file.write_text(value)
    file.chmod(0o600)


def main():
    """Write Earthdata credentials as secrets for Docker.

    Looks for EDL credentials in the environment, and if not found, prompts
    user to enter values, then writes them as secret files for Docker to use.
    """
    env = read_env()
    secrets_dir = ensure_secrets_dir()

    descriptors = (
        ("EARTHDATA_USERNAME", "Earthdata username: ", False),
        ("EARTHDATA_PASSWORD", "Earthdata password: ", True),
    )

    for name, prompt, sensitive in descriptors:
        value = env.get(name) or ask_user(prompt, sensitive)
        write_secret(secrets_dir, to_kebab_case(name), value)


if __name__ == "__main__":
    main()
