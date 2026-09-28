#!/usr/bin/env sh
# Runs Maven inside Docker, so no JDK or Maven is needed on the host.
# Dependencies are cached in the "trading-trends-m2" Docker volume.
#
#   scripts/mvn.sh test                      # all modules
#   scripts/mvn.sh -pl flink-job test        # one module
#   scripts/mvn.sh -pl flink-job test -Dtest=EmaCalculatorTest
set -eu
cd "$(dirname "$0")/.."
exec docker run --rm \
  -v "$PWD":/src \
  -v trading-trends-m2:/root/.m2 \
  -w /src \
  maven:3.9-eclipse-temurin-17 \
  mvn -B -ntp "$@"
