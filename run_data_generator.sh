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

LDAP="${USER}"
DEFAULT_PROJECT="change-stream-hackathon"
DEFAULT_INSTANCE="test-instance-nam3"

MODE="$1"

if [ "$MODE" == "--default" ]; then
  test ! "${LDAP}" && echo "Missing user, make sure your USER environment variable is set" && exit 1
  mvn \
    clean \
    compile \
    exec:java -Dexec.mainClass=com.google.changestreams.sample.DataGeneratorMain \
    -Dexec.args="--project=${DEFAULT_PROJECT} --instance=${DEFAULT_INSTANCE} --ldap=${LDAP}"
else
  mvn \
    clean \
    compile \
    exec:java -Dexec.mainClass=com.google.changestreams.sample.DataGeneratorMain \
    -Dexec.args="$*"
fi
