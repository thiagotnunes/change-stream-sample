#!/usr/bin/env bash

set -e

function print_usage() {
  cat <<EOF
Usage: ./run.sh
  -p <project>
  -i <instance>
  -d <database>
  -mi <metadata instance>
  -md <metadata database>
  -c <change stream name>
  -g <gcs bucket>
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

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
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
    -g|--gcs-bucket)
      GCS_BUCKET="$2"
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

test ! "${PROJECT}" && echo "Missing project" && print_usage
test ! "${INSTANCE}" && echo "Missing instance" && print_usage
test ! "${DATABASE}" && echo "Missing database" && print_usage
test ! "${METADATA_INSTANCE}" && echo "Missing metadata-instance" && print_usage
test ! "${METADATA_DATABASE}" && echo "Missing metadata-database" && print_usage
test ! "${CHANGE_STREAM_NAME}" && echo "Missing change-stream-name" && print_usage
test ! "${GCS_BUCKET}" && echo "Missing gcs-bucket" && print_usage
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
    --gcsBucket=${GCS_BUCKET} \
    --gcpTempLocation=gs://${GCS_BUCKET}/temp \
    --region=${REGION} \
    --runner=DataflowRunner \
    --numWorkers=1 \
    --maxNumWorkers=1 \
    --experiments=use_unified_worker,use_runner_v2,runner_harness_container_image=gcr.io/cloud-dataflow/v1beta3/unified-harness:20211003-rc00 \
  "
