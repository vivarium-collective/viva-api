#!/bin/bash
set -euo pipefail

# Resolve root of repo after GitHub Actions checkout
ROOT_DIR="${GITHUB_WORKSPACE}"

# Make sure version.py exists
VERSION_FILE="${ROOT_DIR}/viva_api/version.py"
if [[ ! -f "$VERSION_FILE" ]]; then
  echo "ERROR: version.py not found at $VERSION_FILE"
  exit 1
fi

# Extract version
declared_version=$(grep -oE '__version__ = \"[^\"]+\"' "$VERSION_FILE" | awk -F'"' '{print $2}')
version=${1:-${declared_version}}
service="$2"

echo "building and pushing images for version ${version}"

function build_service {
  local version="$1"
  tag="${version}"
  dockerfile="${ROOT_DIR}/Dockerfile-${service}"
  image_name="ghcr.io/vivarium-collective/sms-${service}:${tag}"
  if [[ ! -f "$dockerfile" ]]; then
    echo "ERROR: Dockerfile not found: $dockerfile"
    exit 1
  fi
  echo "Building $image_name using $dockerfile"
  docker buildx build \
    --platform=linux/amd64 \
    -f "${dockerfile}" \
    --tag "${image_name}" \
    --push \
    "${ROOT_DIR}" \
    || { echo "Failed to build ${service}"; exit 1; }
  echo "Built and pushed service ${service} version ${version}"
}

function build_all {
  for service in api,ptools; do
    tag="${version}"
    dockerfile="${ROOT_DIR}/Dockerfile-${service}"
    image_name="ghcr.io/vivarium-collective/sms-${service}:${tag}"

    if [[ ! -f "$dockerfile" ]]; then
      echo "ERROR: Dockerfile not found: $dockerfile"
      exit 1
    fi

    echo "Building $image_name using $dockerfile"
    docker buildx build \
      --platform=linux/amd64 \
      -f "${dockerfile}" \
      --tag "${image_name}" \
      --push \
      "${ROOT_DIR}" \
      || { echo "Failed to build ${service}"; exit 1; }

    echo "Built and pushed service ${service} version ${version}"
  done
}


[ -n "$service" ] && build_service "$service" || build_all
