#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version="${1:?Usage: scripts/package.sh vX.Y.Z}"
[[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Use a semantic version tag' >&2; exit 2; }
git rev-parse --verify "refs/tags/$version" >/dev/null
mkdir -p dist
git archive --format=zip --prefix="kafka-lab-$version/" --output="dist/kafka-lab-$version.zip" "refs/tags/$version"
git archive --format=tar.gz --prefix="kafka-lab-$version/" --output="dist/kafka-lab-$version.tar.gz" "refs/tags/$version"
cd dist
sha256sum "kafka-lab-$version.zip" "kafka-lab-$version.tar.gz" > SHA256SUMS
sha256sum --check SHA256SUMS
