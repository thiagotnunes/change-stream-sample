#!/usr/bin/env bash

set -e

function print_usage() {
  cat <<EOF
Usage: ./run_pipeline.sh
  -p <project>
  -i <instance>
  -d <database>
  -mi <metadata instance>
  -md <metadata database>
  -c <change stream name>
  -g <gcs path>
  -r <job region>
EOF
  exit
}

if ! command -v mvn &>/dev/null; then
	cat <<EOF
Please install Maven!

Linux:  sudo apt install -y maven
Mac:  brew install maven
EOF
	exit
fi

LDAP="${USER}"

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --default)
      MODE="--default"
      break
    ;;
    -p|--project)
      PROJECT="$2"
      shift
      shift
      ;;
    -i|--instance)
      INSTANCE="$2"
      shift
      shift
      ;;
    -d|--database)
      DATABASE="$2"
      shift
      shift
      ;;
    -mi|--metadata-instance)
      METADATA_INSTANCE="$2"
      shift
      shift
      ;;
    -md|--metadata-database)
      METADATA_DATABASE="$2"
      shift
      shift
      ;;
    -c|--change-stream-name)
      CHANGE_STREAM_NAME="$2"
      shift
      shift
      ;;
    -g|--gcs-path)
      GCS_PATH="$2"
      shift
      shift
      ;;
    -r|--region)
      REGION="$2"
      shift
      shift
      ;;
  *)
    echo "Unknown option $1"
    print_usage
    ;;
  esac
done

if [ "$MODE" == "--default" ]; then
  test ! "${LDAP}" && echo "Missing user, make sure your USER environment variable is set" && exit 1
  PROJECT="change-stream-hackathon"
  INSTANCE="test-instance-nam3"
  DATABASE="${LDAP}-hackathon"
  METADATA_INSTANCE="${INSTANCE}"
  METADATA_DATABASE="${DATABASE}"
  CHANGE_STREAM_NAME="${LDAP}ChangeStream"
  GCS_PATH="gs://change-streams-hackathon/${LDAP}"
  REGION="us-central1"
fi

test ! "${PROJECT}" && echo "Missing project" && print_usage
test ! "${INSTANCE}" && echo "Missing instance" && print_usage
test ! "${DATABASE}" && echo "Missing database" && print_usage
test ! "${METADATA_INSTANCE}" && echo "Missing metadata-instance" && print_usage
test ! "${METADATA_DATABASE}" && echo "Missing metadata-database" && print_usage
test ! "${CHANGE_STREAM_NAME}" && echo "Missing change-stream-name" && print_usage
test ! "${GCS_PATH}" && echo "Missing gcs-path" && print_usage
test ! "${REGION}" && echo "Missing region" && print_usage

mvn \
  clean \
  compile \
  exec:java -Dexec.mainClass=com.google.changestreams.sample.PipelineMain \
  -Dexec.args=" \
    --project=${PROJECT} \
    --instance=${INSTANCE} \
    --database=${DATABASE} \
    --metadataInstance=${METADATA_INSTANCE} \
    --metadataDatabase=${METADATA_DATABASE} \
    --changeStreamName=${CHANGE_STREAM_NAME} \
    --gcsPath=${GCS_PATH} \
    --gcpTempLocation=${GCS_PATH}/temp \
    --region=${REGION} \
    --runner=DataflowRunner \
    --numWorkers=1 \
    --maxNumWorkers=1 \
    --experiments=use_unified_worker,use_runner_v2
  "
