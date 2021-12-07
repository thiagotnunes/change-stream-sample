# Change Streams Dataflow Sample

In this repository we show a sample Dataflow pipeline using the Cloud Spanner Change Streams Connector.
In this repository we also show a data generator class to generate load in a database, so you can test the Change Streams consumption.

## Connector

In this section we go over the Google Dataflow Connector sample.

### Requirements

- You must have [maven](https://maven.apache.org/download.cgi) installed.
- You must have set up [authentication](https://cloud.google.com/docs/authentication/getting-started) for gcloud.
    - The authenticated account must have access to start a Dataflow job (more at [Cloud Dataflow IAM](https://cloud.google.com/dataflow/docs/concepts/access-control)).
    - The authenticated account must have access to read / write to the specified GCS bucket (more at [Cloud Storage IAM](https://cloud.google.com/storage/docs/access-control/iam-roles)).
    - The authenticated account must have access to query the Cloud Spanner Change Stream in the project/instance/database used (more at [Cloud Spanner IAM](https://cloud.google.com/spanner/docs/iam)).
    - The authenticated account must have access to update Cloud Spanner database ddl in project/metadata instance/metadata database used (more at [Cloud Spanner IAM](https://cloud.google.com/spanner/docs/iam)).
- You must have set up [dataflow security and permissions](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#security_and_permissions_for_local_pipelines) correctly.
- You must have pre-created a Cloud Spanner change stream to be read from.
- You must have pre-installed the Apache Beam Connector jar. You can see the specified jar version in the application `pom.xml` file, under `connector.version`.

### Application

The application specified in the `com.google.changestreams.sample.PipelineMain` will perform the following:

1. It will read the specified change stream for 10 minutes of data (from now to 10 minutes in the future).
2. It will extract the commit timestamps of each record streamed.
3. It will group the records in 1 minute windows.
4. It will output each window group into a separate file in GCS.

### How to Run

We provided a bash script to facilitate the execution. You can execute it like so:

```bash
./run_pipeline.sh \
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
- `-mi|--metadata-instance`: the Google Cloud Spanner instance id where the Connector metadata tables will be created (we recommend it to be different than the change stream instance)
- `-md|--metadata-database`: the Google Cloud Spanner database id where the Connector metadata tables will be created (we recommend it to be different than the change stream database)
- `-c|--change-stream-name`: the name of the pre-created Google Cloud Spanner change stream
- `-g|--gcs-bucket`: the Google Cloud Storage bucket to be used to store the results of the pipeline and to stage temp files for the Dataflow execution
- `-r|--region`: the region where to execute the Dataflow job (for options see [Dataflow Locations](https://cloud.google.com/dataflow/docs/resources/locations))

The job executed here will spawn a single Dataflow worker to consume the change stream.

## Data Generator

In this section we go over the Data Generator sample.

### Requirements

- You must have [maven](https://maven.apache.org/download.cgi) installed.
- You must have set up [authentication](https://cloud.google.com/docs/authentication/getting-started) for gcloud.
  - The authenticated account must have access to create and query the Cloud Spanner Change Stream in the project/instance/database used (more at [Cloud Spanner IAM](https://cloud.google.com/spanner/docs/iam)).
  - The authenticated account must have access to update Cloud Spanner database ddl in project/metadata instance/metadata database used (more at [Cloud Spanner IAM](https://cloud.google.com/spanner/docs/iam)).

### Application

The application specified in the `com.google.changestreams.sample.DataGeneratorMain` will perform the following:

1. The user needs to provide a `project`, `instance` and `ldap`.
2. It will retrieve a database with the id `<ldap>-hackaton`. If this database does not exist it will be created.
3. It will retrieve a table `Singers` within the `<ldap>-hackaton` database. If this table does not exist it will be created. The `Singers` table schema is the following:

```sql
CREATE TABLE Singers (
  SingerId   INT64 NOT NULL,
  FirstName  STRING(1024),
  LastName   STRING(1024),
  SingerInfo BYTES(MAX)
  ) PRIMARY KEY (SingerId)
```

4. It will retrieve a change stream with the name `<ldap>ChangeStream` within the `<ldap>-hackaton` database. If the change stream does not exist it will be created. The `<ldap>ChangeStream` will be created as follows:

```sql
CREATE CHANGE STREAM <ldap>ChangeStream FOR Singers
```

5. It will do 10 blind writes (insert or update mutations) to the `Singers` table and it sleep for 1 second. This will continue forever until the user exits (`CTRL-C`).

### How to Run

We provided a bash script for executing the application:

```bash
./run_data_generator.sh \
  --project <my-gcp-project> \
  --instance <my-spanner-instance> \
  --ldap <my-ldap>
```

The parameters are describe below:

- `-p|--project`: the Google Cloud Platform project id
- `-i|--instance`: the Google Cloud Spanner instance id where the change stream resides
- `-l|--ldap`: your Google LDAP

The application will be executed in your local machine.
