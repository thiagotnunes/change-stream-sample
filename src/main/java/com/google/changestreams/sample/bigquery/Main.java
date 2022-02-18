package com.google.changestreams.sample.bigquery;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.changestreams.sample.SampleOptions;
import com.google.cloud.Timestamp;
import io.opencensus.trace.samplers.Samplers;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Main {

  private static final String BQ_SCHEMA_NAME_PARTITION_TOKEN = "partition_token";
  private static final String BQ_SCHEMA_NAME_TRANSACTION_ID = "server_transaction_id";
  private static final String BQ_SCHEMA_NAME_RECORD = "record";
  private static final String BQ_SCHEMA_NAME_COMMIT_TIMESTAMP = "commit_timestamp";
  private static final String BQ_SCHEMA_NAME_EMIT_TIMESTAMP = "emit_timestamp";
  private static final String BQ_SCHEMA_NAME_BQ_TIMESTAMP = "bq_timestamp";

  private static class RecordWithMetadata implements Serializable {

    public DataChangeRecord dataChangeRecord;
    public Timestamp emittedTime;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecordWithMetadata)) {
        return false;
      }
      RecordWithMetadata that = (RecordWithMetadata) o;
      return dataChangeRecord.equals(that.dataChangeRecord) && emittedTime.equals(that.emittedTime);
    }
  }

  public static void main(String[] args) {

    final SampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .as(SampleOptions.class);

    options.setFilesToStage(deduplicateFilesToStage(options));
    options.setStreaming(true);

    final Pipeline pipeline = Pipeline.create(options);

    String projectId = options.getProject();
    String instanceId = options.getInstance();
    String databaseId = options.getDatabase();
    String metadataInstanceId = options.getMetadataInstance();
    String metadataDatabaseId = options.getMetadataDatabase();
    String changeStreamName = options.getChangeStreamName();
    String bigQueryDataset = options.getBigQueryDataset();
    String bigQueryTableName = options.getBigQueryTableName();

    final String bigQueryTable = String
        .format("%s:%s.%s", projectId, bigQueryDataset, bigQueryTableName);

    final Timestamp inclusiveStartAt = Timestamp.now();
    final Timestamp inclusiveEndAt = Timestamp.ofTimeSecondsAndNanos(
        inclusiveStartAt.getSeconds() + (60 * 60),
        inclusiveStartAt.getNanos()
    );

    pipeline
        // Reads from the change stream
        .apply(SpannerIO
            .readChangeStream()
            .withSpannerConfig(SpannerConfig
                .create()
                .withHost(StaticValueProvider.of("https://staging-wrenchworks.sandbox.googleapis.com"))
                .withProjectId(projectId)
                .withInstanceId(instanceId)
                .withDatabaseId(databaseId)
            )
            .withMetadataInstance(metadataInstanceId)
            .withMetadataDatabase(metadataDatabaseId)
            .withChangeStreamName(changeStreamName)
            .withInclusiveStartAt(inclusiveStartAt)
            .withInclusiveEndAt(inclusiveEndAt)
        )

        .apply("Add emitted time to records",
            MapElements.into(TypeDescriptor.of(RecordWithMetadata.class)).via(record -> {
                  RecordWithMetadata recordWithMetadata = new RecordWithMetadata();
                  recordWithMetadata.dataChangeRecord = record;
                  recordWithMetadata.emittedTime = Timestamp.now();
                  return recordWithMetadata;
                }
            ))

        // Writes each window of records into BigQuery
        .apply("Write to BigQuery table",
            BigQueryIO
                .<RecordWithMetadata>write()
                .to(bigQueryTable)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                .withSchema(createSchema())
                .withAutoSharding()
                // Uncomment line below for BigQuery Storage Write API
                // .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .optimizedWrites()
                .withFormatFunction((RecordWithMetadata elem) ->
                    new TableRow()
                        .set(BQ_SCHEMA_NAME_PARTITION_TOKEN,
                            elem.dataChangeRecord.getPartitionToken())
                        .set(BQ_SCHEMA_NAME_TRANSACTION_ID, elem.dataChangeRecord.getServerTransactionId())
                        .set(BQ_SCHEMA_NAME_RECORD, elem.dataChangeRecord.toString())
                        .set(BQ_SCHEMA_NAME_COMMIT_TIMESTAMP,
                            elem.dataChangeRecord.getCommitTimestamp().getSeconds())
                        .set(BQ_SCHEMA_NAME_EMIT_TIMESTAMP, elem.emittedTime.getSeconds())
                        .set(BQ_SCHEMA_NAME_BQ_TIMESTAMP, "AUTO")
                )
        );

    pipeline.run().waitUntilFinish();
  }

  private static TableSchema createSchema() {
    return new TableSchema().setFields(
        Arrays.asList(
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_PARTITION_TOKEN)
                .setType("STRING")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_TRANSACTION_ID)
                .setType("STRING")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_COMMIT_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_EMIT_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_BQ_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
        )
    );
  }

  /**
   * This is to avoid a bug in Dataflow, where if there are duplicate jar files to stage, the job
   * gets stuck. Before submitting the job we deduplicate the jar files here.
   */
  private static List<String> deduplicateFilesToStage(DataflowPipelineOptions options) {
    final Map<String, String> fileNameToPath = new HashMap<>();
    final List<String> filePaths =
        detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), options);

    for (String filePath : filePaths) {
      final File file = new File(filePath);
      final String fileName = file.getName();
      if (!fileNameToPath.containsKey(fileName)) {
        fileNameToPath.put(fileName, filePath);
      }
    }

    return new ArrayList<>(fileNameToPath.values());
  }
}
