#!/usr/bin/env bash

set -e

if ! command -v mvn &>/dev/null; then
	cat <<EOF
Please install Maven!

Linux:  sudo apt install -y maven
Mac:  brew install maven
EOF
	exit
fi

mvn \
  install:install-file \
  -Dfile=artifacts/beam-sdks-java-io-google-cloud-platform-2.33.0.1-SNAPSHOT.jar \
  -DpomFile=artifacts/pom.xml

