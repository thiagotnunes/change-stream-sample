#!/usr/bin/env bash

set -e

function print_usage() {
  cat <<EOF
Usage: ./run.sh
  -p <project>
  -g <gcs bucket>
  -r <job region>
  -bd <BigQuery dataset>
  -bt <BigQuery table name>
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
    -bd|--big-query-dataset)
      BIG_QUERY_DATASET="$2"
      shift
      shift
      ;;
    -bt|--big-query-table-name)
      BIG_QUERY_TABLE_NAME="$2"
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
test ! "${GCS_BUCKET}" && echo "Missing gcs-bucket" && print_usage
test ! "${REGION}" && echo "Missing region" && print_usage

mvn \
  clean \
  compile \
  exec:java -Dexec.mainClass=com.google.changestreams.sample.bigquery.ReadFromText \
  -Dexec.args=" \
    --project=${PROJECT} \
    --gcsBucket=${GCS_BUCKET} \
    --gcpTempLocation=gs://${GCS_BUCKET}/temp \
    --region=${REGION} \
    --bigQueryDataset=${BIG_QUERY_DATASET} \
    --bigQueryTableName=${BIG_QUERY_TABLE_NAME} \
    --runner=DataflowRunner \
    --numWorkers=10 \
    --maxNumWorkers=10 \
    --experiments=use_unified_worker,use_runner_v2 \
  "
