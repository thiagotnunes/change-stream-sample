# Change Streams Dataflow Sample

In this repository we show a sample Dataflow pipeline using the Cloud Spanner Change Streams Connector.

## Requirements

- You must have [maven](https://maven.apache.org/download.cgi) installed.
- You must have set up [authentication](https://cloud.google.com/docs/authentication/getting-started) for gcloud.
    - The authenticated account must have access to start a Dataflow job (more at [Cloud Dataflow IAM](https://cloud.google.com/dataflow/docs/concepts/access-control)).
    - The authenticated account must have access to read / write to the specified GCS bucket (more at [Cloud Storage IAM](https://cloud.google.com/storage/docs/access-control/iam-roles)).
    - The authenticated account must have access to query the Cloud Spanner Change Stream in the project/instance/database used (more at [Cloud Spanner IAM](https://cloud.google.com/spanner/docs/iam)).
    - The authenticated account must have access to update Cloud Spanner database ddl in project/metadata instance/metadata database used (more at [Cloud Spanner IAM](https://cloud.google.com/spanner/docs/iam)).
- You must have set up [dataflow security and permissions](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#security_and_permissions_for_local_pipelines) correctly.
- You must have pre-created a Cloud Spanner change stream to be read from.
- You must have pre-installed the Apache Beam Connector jar. You can see the specified jar version in the application `pom.xml` file, under `connector.version`.

## Application

The application specified in the `com.google.changestreams` will perform the following:

1. It will read the specified change stream for 10 minutes of data (from now to 10 minutes in the future).
2. It will extract the commit timestamps of each record streamed.
3. It will group the records in 1 minute windows.
4. It will output each window group into either a separate file in GCS, or records in BigQuery.

## How to Run

We provided a bash script to facilitate the execution.

### GCS
For GCS, you can execute the script like so:

```bash
./run_gcs.sh \
  --project <my-gcp-project> \
  --instance <my-spanner-instance> \
  --database <my-spanner-database> \
  --metadata-instance <my-spanner-metadata-instance> \
  --metadata-database <my-spanner-metadata-database> \
  --change-stream-name <my-spanner-change-stream-name> \
  --gcs-bucket <my-gcs-bucket> \
  --region <my-dataflow-job-region>
```

This script will dispatch a remote job in dataflow with the specified configuration:

- `-p|--project`: the Google Cloud Platform project id
- `-i|--instance`: the Google Cloud Spanner instance id where the change stream resides
- `-d|--database`: the Google Cloud Spanner database id where the change stream resides
- `-mi|--metadata-instance`: the Google Cloud Spanner instance id where the Connector metadata tables will be created
- `-md|--metadata-database`: the Google Cloud Spanner database id where the Connector metadata tables will be created (we recommend it to be different from the change stream database)
- `-c|--change-stream-name`: the name of the pre-created Google Cloud Spanner change stream
- `-g|--gcs-bucket`: the Google Cloud Storage bucket to be used to store the results of the pipeline and to stage temp files for the Dataflow execution
- `-r|--region`: the region where to execute the Dataflow job (for options see [Dataflow Locations](https://cloud.google.com/dataflow/docs/resources/locations))

The job executed here will spawn a single Dataflow worker to consume the change stream.

### BigQuery
For BigQuery, you can execute the script like so:

```bash
./run_bigquery.sh \
  --project <my-gcp-project> \
  --instance <my-spanner-instance> \
  --database <my-spanner-database> \
  --metadata-instance <my-spanner-metadata-instance> \
  --metadata-database <my-spanner-metadata-database> \
  --change-stream-name <my-spanner-change-stream-name> \
  --gcs-bucket <my-gcs-bucket> \
  --big-query-dataset <my-big-query-dataset> \
  --big-query-table-name <my-big-query-table-name> \
  --region <my-dataflow-job-region>
```

This script will dispatch a remote job in dataflow with the specified configuration:

- `-p|--project`: the Google Cloud Platform project id
- `-i|--instance`: the Google Cloud Spanner instance id where the change stream resides
- `-d|--database`: the Google Cloud Spanner database id where the change stream resides
- `-mi|--metadata-instance`: the Google Cloud Spanner instance id where the Connector metadata tables will be created
- `-md|--metadata-database`: the Google Cloud Spanner database id where the Connector metadata tables will be created (we recommend it to be different from the change stream database)
- `-c|--change-stream-name`: the name of the pre-created Google Cloud Spanner change stream
- `-g|--gcs-bucket`: the Google Cloud Storage bucket to be used to stage temp files for the Dataflow execution
- `-bd|--big-query-dataset`: the BigQuery dataset to store the records emitted by the change stream
- `-bt|--big-query-table-name`: the BigQuery table name in the big query dataset to store the records emitted by the change stream
- `-r|--region`: the region where to execute the Dataflow job (for options see [Dataflow Locations](https://cloud.google.com/dataflow/docs/resources/locations))

The job executed here will spawn a single Dataflow worker to consume the change stream.

### Pubsub
For Pubsub, you can execute the script like so:

```bash
./run_pubsub.sh \
  --project <my-gcp-project> \
  --instance <my-spanner-instance> \
  --database <my-spanner-database> \
  --metadata-instance <my-spanner-metadata-instance> \
  --metadata-database <my-spanner-metadata-database> \
  --change-stream-name <my-spanner-change-stream-name> \
  --gcs-bucket <my-gcs-bucket> \
  --pubsub-topic <my-pubsub-topic> \
  --region <my-dataflow-job-region>
```

This script will dispatch a remote job in dataflow with the specified configuration:

- `-p|--project`: the Google Cloud Platform project id
- `-i|--instance`: the Google Cloud Spanner instance id where the change stream resides
- `-d|--database`: the Google Cloud Spanner database id where the change stream resides
- `-mi|--metadata-instance`: the Google Cloud Spanner instance id where the Connector metadata tables will be created
- `-md|--metadata-database`: the Google Cloud Spanner database id where the Connector metadata tables will be created (we recommend it to be different from the change stream database)
- `-c|--change-stream-name`: the name of the pre-created Google Cloud Spanner change stream
- `-g|--gcs-bucket`: the Google Cloud Storage bucket to be used to stage temp files for the Dataflow execution
- `-t|--pubsub-topic`: the Google Cloud Pubsub topic to be used to publish the results of the pipeline
- `-r|--region`: the region where to execute the Dataflow job (for options see [Dataflow Locations](https://cloud.google.com/dataflow/docs/resources/locations))

The job executed here will spawn a single Dataflow worker to consume the change stream.
