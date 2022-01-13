package com.google.changestreams.sample.bigquery;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.changestreams.sample.SampleOptions;
import com.google.cloud.Timestamp;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.cloud.spanner.*;
import org.apache.avro.reflect.AvroEncode;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampEncoding;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.*;

public class Main {
  // Fields of BigQuery changelog table.
  private static final String BQ_SCHEMA_NAME_SINGER_ID = "singer_id";
  private static final String BQ_SCHEMA_NAME_FIRST_NAME = "first_name";
  private static final String BQ_SCHEMA_NAME_LAST_NAME = "last_name";
  private static final String BQ_SCHEMA_NAME_MOD_TYPE = "mod_type";
  private static final String BQ_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP = "spanner_commit_timestamp";
  private static final String BQ_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP = "bq_commit_timestamp";

  public static void main(String[] args) {

    final SampleOptions options = PipelineOptionsFactory
      .fromArgs(args)
      .as(SampleOptions.class);

    options.setFilesToStage(deduplicateFilesToStage(options));
    options.setEnableStreamingEngine(true);
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

    final Timestamp now = Timestamp.now();
    final Timestamp after1Hour = Timestamp.ofTimeSecondsAndNanos(
      now.getSeconds() + (60 * 60),
      now.getNanos()
    );

    pipeline
      // Read from the change stream.
      .apply("Read from change stream",
        SpannerIO
        .readChangeStream()
        .withSpannerConfig(SpannerConfig
          .create()
          .withProjectId(projectId)
          .withInstanceId(instanceId)
          .withDatabaseId(databaseId)
        )
        .withMetadataInstance(metadataInstanceId)
        .withMetadataDatabase(metadataDatabaseId)
        .withChangeStreamName(changeStreamName)
        .withInclusiveStartAt(now)
        .withInclusiveEndAt(after1Hour)
      )

      // Converts DataChangeRecord to SingerRow, each DataChangeRecord may contain
      // multiple SingerRow, since it has multiple Mod.
      .apply(ParDo.of(new ChangeRecordToSingersFn()))

      // Writes SingerRow into BigQuery changelog table.
      .apply("Write to BigQuery changelog table",
        BigQueryIO
          .<SingerRow>write()
          .to(bigQueryTable)
          // Use streaming insert, note streaming insert also provides instance data availability
          // for query (not for DML).
          .withMethod(Write.Method.STREAMING_INSERTS)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
          .withSchema(createSchema())
          .withAutoSharding()
          .optimizedWrites()
          .withFormatFunction((SingerRow s) -> {
              return new TableRow()
                .set(BQ_SCHEMA_NAME_SINGER_ID, s.singerId)
                .set(BQ_SCHEMA_NAME_FIRST_NAME, s.firstName)
                .set(BQ_SCHEMA_NAME_LAST_NAME, s.lastName)
                .set(BQ_SCHEMA_NAME_MOD_TYPE, s.modType)
                .set(BQ_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP, s.commitTimestamp.getSeconds())
                // When using "AUTO", BigQuery will automatically populate the commit timestamp.
                .set(BQ_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP, "AUTO");
            }
          )
      );

    pipeline.run().waitUntilFinish();
  }

  private static TableSchema createSchema() {
    return new TableSchema().setFields(
      Arrays.asList(
        new TableFieldSchema()
          .setName(BQ_SCHEMA_NAME_SINGER_ID)
          .setType("INT64")
          .setMode("REQUIRED"),
        new TableFieldSchema()
          .setName(BQ_SCHEMA_NAME_FIRST_NAME)
          .setType("STRING")
          .setMode("NULLABLE"),
        new TableFieldSchema()
          .setName(BQ_SCHEMA_NAME_LAST_NAME)
          .setType("STRING")
          .setMode("NULLABLE"),
        new TableFieldSchema()
          .setName(BQ_SCHEMA_NAME_MOD_TYPE)
          .setType("STRING")
          .setMode("REQUIRED"),
        new TableFieldSchema()
          .setName(BQ_SCHEMA_NAME_SPANNER_COMMIT_TIMESTAMP)
          .setType("TIMESTAMP")
          .setMode("REQUIRED"),
        new TableFieldSchema()
          .setName(BQ_SCHEMA_NAME_BQ_COMMIT_TIMESTAMP)
          .setType("TIMESTAMP")
          .setMode("REQUIRED")
      )
    );
  }

  @DefaultCoder(AvroCoder.class)
  static class SingerRow implements Serializable {
    public int singerId;
    @AvroEncode(
      using = TimestampEncoding.class
    )
    com.google.cloud.Timestamp commitTimestamp;
    public String firstName, lastName, modType;
  }

  static class ChangeRecordToSingersFn extends DoFn<DataChangeRecord, SingerRow> {
    @ProcessElement public void process(@Element DataChangeRecord element,
                                        OutputReceiver<SingerRow> out,
                                        ProcessContext c) {
      SampleOptions ops = c.getPipelineOptions().as(SampleOptions.class);
      Spanner spanner =
        SpannerOptions.newBuilder()
          .setProjectId(ops.getProject())
          .build()
          .getService();
      DatabaseClient databaseClient = spanner
        .getDatabaseClient(DatabaseId.of(ops.getProject(), ops.getInstance(), ops.getDatabase()));

      String modType = element.getModType().toString();
      com.google.cloud.Timestamp commitTimestamp = element.getCommitTimestamp();
      for (Mod mod : element.getMods()) {
        SingerRow s = new SingerRow();
        s.commitTimestamp = commitTimestamp;
        s.modType = modType;
        JSONObject json = new JSONObject(mod.getKeysJson());
        int singerId = json.getInt("SingerId");
        s.singerId = singerId;
        try (ResultSet resultSet =
               databaseClient
                 .singleUse()
                 .read(
                   "Singers",
                   KeySet.singleKey(com.google.cloud.spanner.Key.of(singerId)),
                   Arrays.asList("FirstName", "LastName"))) {

          // We will only receive one row.
          while (resultSet.next()) {
            s.firstName = resultSet.getString("FirstName");
            s.lastName = resultSet.getString("LastName");
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        out.output(s);
      }

      spanner.close();
    }
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