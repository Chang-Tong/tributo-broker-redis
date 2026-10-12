"""Use Ray's native py_modules upload for inputs too large for process env."""

from __future__ import annotations

import importlib.resources
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

DRIVER_ENV = "TRIBUTO_REDIS_DRIVER_INPUT_B64"
RESOURCE_ENV = "TRIBUTO_REDIS_DRIVER_INPUT_RESOURCE"
PACKAGE = "tributo_ray_driver_input"
INLINE_LIMIT = 48 * 1024


@contextmanager
def driver_transport(
    encoded: str, env_vars: Mapping[str, str], modules: list[str]
) -> Iterator[tuple[dict[str, str], list[str]]]:
    """Keep files until submit_job finishes its synchronous package upload.

    Linux limits each argument/environment string independently of ARG_MAX.
    Splitting env vars is insufficient: Ray also serializes runtime_env for
    worker startup. A small native Ray package avoids both process limits.
    """
    env = dict(env_vars)
    env.pop(DRIVER_ENV, None)
    env.pop(RESOURCE_ENV, None)
    if len(encoded) <= INLINE_LIMIT:
        env[DRIVER_ENV] = encoded
        yield env, modules
        return
    with TemporaryDirectory(prefix="tributo-driver-") as directory:
        package = Path(directory) / PACKAGE
        package.mkdir(mode=0o700)
        (package / "__init__.py").write_text("", encoding="utf-8")
        payload = package / "input.b64"
        payload.touch(mode=0o600)
        payload.write_text(encoded, encoding="ascii")
        env[RESOURCE_ENV] = PACKAGE
        yield env, [*modules, str(package)]


def read_driver_input(env: Mapping[str, str]) -> str:
    inline = env.get(DRIVER_ENV)
    resource = env.get(RESOURCE_ENV)
    if inline is not None and resource is not None:
        raise ValueError("driver input has conflicting transports")
    if inline is not None:
        return inline
    if resource is not None:
        if resource != PACKAGE:
            raise ValueError("driver input resource is not recognized")
        return (
            importlib.resources.files(PACKAGE)
            .joinpath("input.b64")
            .read_text(encoding="ascii")
        )
    raise ValueError(f"{DRIVER_ENV} is required (or native Ray input package)")
