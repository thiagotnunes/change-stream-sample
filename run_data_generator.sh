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
  clean \
  compile \
  exec:java -Dexec.mainClass=com.google.changestreams.sample.DataGeneratorMain \
  -Dexec.args="$*"
