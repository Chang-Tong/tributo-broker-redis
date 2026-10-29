#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

project_name="tributo-broker-redis-it-$(date +%Y%m%d%H%M%S)-$$"
compose_file="$repo_root/tests/integrations/docker-compose.yml"
wheel_dir=$(mktemp -d /tmp/tributo-broker-redis-wheel.XXXXXX)
ray_storage_dir=$(mktemp -d /tmp/tributo-broker-ray-results.XXXXXX)
venv_dir=$(mktemp -d /tmp/tributo-broker-redis-venv.XXXXXX)
core_only_venv_dir=$(mktemp -d /tmp/tributo-core-only-venv.XXXXXX)
core_wheel_dir=""
ray_image=${BROKER_RAY_IMAGE:-tributo-runtime-full:local}
chmod 777 "$ray_storage_dir"
project_started=0

cleanup() {
  if [ "$project_started" -eq 1 ]; then
    docker compose --project-name "$project_name" --file "$compose_file" logs --no-color \
      >"$repo_root/tests/integration/.runtime/docker.log" 2>&1 || true
    docker compose --project-name "$project_name" --file "$compose_file" down --volumes --remove-orphans
  fi
  if [ -d "$wheel_dir" ]; then
    mv "$wheel_dir" "/tmp/tributo-broker-redis-wheel-complete-$$" 2>/dev/null || true
  fi
  if [ -d "$ray_storage_dir" ]; then
    mv "$ray_storage_dir" "/tmp/tributo-broker-ray-results-complete-$$" 2>/dev/null || true
  fi
  if [ -d "$venv_dir" ]; then
    mv "$venv_dir" "/tmp/tributo-broker-redis-venv-complete-$$" 2>/dev/null || true
  fi
  if [ -d "$core_only_venv_dir" ]; then
    mv "$core_only_venv_dir" "/tmp/tributo-core-only-venv-complete-$$" 2>/dev/null || true
  fi
  if [ -n "$core_wheel_dir" ] && [ -d "$core_wheel_dir" ]; then
    mv "$core_wheel_dir" "/tmp/tributo-core-wheel-complete-$$" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

uv build --out-dir "$wheel_dir" "$repo_root"
wheel_path=$(find "$wheel_dir" -maxdepth 1 -name 'tributo_broker_redis-*.whl' -print -quit)
test -n "$wheel_path"
wheel_name=$(basename "$wheel_path")

core_root=${TRIBUTO_CORE_ROOT:-}
if [ -z "$core_root" ]; then
  echo "TRIBUTO_CORE_ROOT must point to the compatible Tributo Core worktree" >&2
  exit 2
fi
core_wheel_dir=$(mktemp -d /tmp/tributo-core-wheel.XXXXXX)
uv build --out-dir "$core_wheel_dir" "$core_root"
core_wheel=$(find "$core_wheel_dir" -maxdepth 1 -name 'tributo-*.whl' -print -quit)
test -n "$core_wheel"

uv venv "$venv_dir"
python_bin="$venv_dir/bin/python"
uv pip install --python "$python_bin" "$core_wheel" "${wheel_path}[xgboost]" pytest

uv venv "$core_only_venv_dir"
core_only_python="$core_only_venv_dir/bin/python"
uv pip install --python "$core_only_python" "$core_wheel"
"$core_only_python" -c "import importlib.util; import tributo; assert importlib.util.find_spec('redis') is None"

ray_image_id=$(docker image inspect --format '{{.Id}}' "$ray_image")
ray_image_title=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.title"}}' "$ray_image")
ray_image_version=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$ray_image")
ray_image_ray_version=$(docker image inspect --format '{{index .Config.Labels "org.tributo.ray-version"}}' "$ray_image")
ray_image_manifest=$(docker image inspect --format '{{index .Config.Labels "org.tributo.manifest-sha256"}}' "$ray_image")
core_version=$("$python_bin" -c 'from importlib.metadata import version; print(version("tributo"))')
if [ "$ray_image_title" != "tributo-runtime-full" ]; then
  echo "BROKER_RAY_IMAGE must be built by the Tributo full runtime builder" >&2
  exit 2
fi
if [ "$ray_image_version" != "$core_version" ]; then
  echo "Ray image Tributo version does not match the tested Core wheel" >&2
  exit 2
fi
if [ "$ray_image_ray_version" != "2.55.1" ]; then
  echo "Ray image must attest Ray 2.55.1" >&2
  exit 2
fi
if [ "${#ray_image_manifest}" -ne 64 ]; then
  echo "Ray image is missing a valid Tributo manifest attestation" >&2
  exit 2
fi
case "$ray_image_manifest" in
  *[!0-9a-f]*)
    echo "Ray image has an invalid Tributo manifest attestation" >&2
    exit 2
    ;;
esac
export BROKER_RAY_IMAGE="$ray_image"
runtime_log_dir="$repo_root/tests/integration/.runtime"
mkdir -p "$runtime_log_dir"
printf '%s\n' \
  "image=$ray_image" \
  "image_id=$ray_image_id" \
  "tributo_version=$ray_image_version" \
  "ray_version=$ray_image_ray_version" \
  "manifest_sha256=$ray_image_manifest" \
  >"$runtime_log_dir/ray-image.log"

wheel_discovery_log="$runtime_log_dir/wheel-discovery.log"
mkdir -p "$(dirname "$wheel_discovery_log")"
wheel_test_status=0
"$python_bin" -m pytest -p no:cacheprovider -q \
  tests/integration/test_broker_redis_wheel.py -m integration \
  >"$wheel_discovery_log" 2>&1 || wheel_test_status=$?
tee /dev/stderr <"$wheel_discovery_log"
if [ "$wheel_test_status" -ne 0 ]; then
  exit "$wheel_test_status"
fi

export BROKER_WHEEL_DIR="$wheel_dir"
export BROKER_RAY_STORAGE_DIR="$ray_storage_dir"
export BROKER_PROVIDER_WHEEL_NAME="$wheel_name"
export BROKER_PROJECT_ROOT="$core_root"
export BROKER_FIXTURE_DIR="$repo_root/tests/integration/fixtures"
export BROKER_TRAIN_FIXTURE="/provider-data/broker_train.csv"
export BROKER_TEST_LOG="$repo_root/tests/integration/.runtime/standalone.log"
mkdir -p "$(dirname "$BROKER_TEST_LOG")"
project_started=1
docker compose --project-name "$project_name" --file "$compose_file" up --detach --wait

export PYTHONPYCACHEPREFIX=/tmp/tributo-broker-pyc
export PYTEST_ADDOPTS=""
test_status=0
"$python_bin" -m pytest -p no:cacheprovider -q tests/integration/test_broker_redis.py -m integration \
  >"$BROKER_TEST_LOG" 2>&1 || test_status=$?
tee /dev/stderr <"$BROKER_TEST_LOG"
exit "$test_status"
