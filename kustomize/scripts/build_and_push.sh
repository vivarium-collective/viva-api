#!/bin/bash

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# version is an optional argument, defaults to the version defined in viva_api/version.py'
#
# version.py is of form:
# __version__ = "0.1.0"
declared_version=$(grep -oE '__version__ = \"[^\"]+\"' "${ROOT_DIR}/viva_api/version.py" | awk -F'"' '{print $2}')
version=${1:-${declared_version}}

default_org="vivarium-collective"  # or, "biosimulations"
container_org=${2:-${default_org}}

echo "building and pushing images to ${container_org} for version ${version}"

for service in api ptools; do

  tag="${version}"
  dockerfile="${ROOT_DIR}/Dockerfile-${service}"
  image_name="ghcr.io/${container_org}/sms-${service}:${tag}"

  docker buildx build --platform=linux/amd64 -f ${dockerfile} --tag ${image_name} "${ROOT_DIR}" \
    || { echo "Failed to build ${service}"; exit 1; }

  docker push ${image_name}  \
    || { echo "Failed to push ${service}"; exit 1; }

  echo "built and pushed service ${service} version ${version}"
done
