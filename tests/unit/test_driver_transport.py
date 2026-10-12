import importlib
import sys
from pathlib import Path

import pytest

from tributo_broker_redis.driver_transport import (
    DRIVER_ENV,
    INLINE_LIMIT,
    PACKAGE,
    RESOURCE_ENV,
    driver_transport,
    read_driver_input,
)


@pytest.mark.parametrize("size", [0, 1, INLINE_LIMIT - 1, INLINE_LIMIT])
def test_inline_boundary(size):
    payload = "x" * size
    with driver_transport(
        payload, {RESOURCE_ENV: "stale", "PATH": "/bin"}, ["/existing"]
    ) as (env, modules):
        assert read_driver_input(env) == payload
        assert RESOURCE_ENV not in env
        assert modules == ["/existing"]
        assert env["PATH"] == "/bin"


@pytest.mark.parametrize("size", [INLINE_LIMIT + 1, 131072, 262144])
def test_large_native_package_roundtrip_and_cleanup(size, monkeypatch):
    payload = "x" * size
    with driver_transport(payload, {DRIVER_ENV: "stale"}, []) as (env, modules):
        assert DRIVER_ENV not in env
        assert env[RESOURCE_ENV] == PACKAGE
        package = Path(modules[0])
        monkeypatch.syspath_prepend(str(package.parent))
        importlib.invalidate_caches()
        sys.modules.pop(PACKAGE, None)
        assert read_driver_input(env) == payload
        assert package.joinpath("input.b64").stat().st_mode & 0o777 == 0o600
        sys.modules.pop(PACKAGE, None)
    assert not package.exists()


def test_cleanup_on_failed_submission():
    with (
        pytest.raises(RuntimeError),
        driver_transport("x" * 60000, {}, []) as (_, modules),
    ):
        path = Path(modules[0])
        raise RuntimeError("upload failed")
    assert not path.exists()


@pytest.mark.parametrize(
    "env",
    [{}, {RESOURCE_ENV: "../../secret"}, {DRIVER_ENV: "x", RESOURCE_ENV: PACKAGE}],
)
def test_reject_invalid_transports(env):
    with pytest.raises(ValueError):
        read_driver_input(env)
