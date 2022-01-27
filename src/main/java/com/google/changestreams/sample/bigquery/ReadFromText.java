package com.google.changestreams.sample.bigquery;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.changestreams.sample.SampleOptions;
import com.google.cloud.Timestamp;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;

public class ReadFromText {

  private static final String BQ_SCHEMA_NAME_PARTITION_TOKEN = "partition_token";
  private static final String BQ_SCHEMA_NAME_COMMITTED_TIMESTAMP = "committed_timestamp";
  private static final String BQ_SCHEMA_NAME_BQ_TIMESTAMP = "bq_timestamp";

  public static void main(String[] args) {

    final SampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .as(SampleOptions.class);

    options.setFilesToStage(deduplicateFilesToStage(options));

    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    final Pipeline pipeline = Pipeline.create(options);

    String projectId = options.getProject();
    String bigQueryDataset = options.getBigQueryDataset();
    String bigQueryTableName = options.getBigQueryTableName();
    String gcsTempLocation = options.getGcpTempLocation();

    final String bigQueryTable = String
        .format("%s:%s.%s", projectId, bigQueryDataset, bigQueryTableName);

    final Timestamp now = Timestamp.now();
    final Timestamp after10Minutes = Timestamp.ofTimeSecondsAndNanos(
        now.getSeconds() + (60 * 60),
        now.getNanos()
    );

    final List<String> lines = Arrays.asList(
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "To be, or not to be: that is the question: ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "Whether 'tis nobler in the mind to suffer ",
        "The slings and arrows of outrageous fortune, ",
        "The slings and arrows of outrageous fortune, ",
        "The slings and arrows of outrageous fortune, ",
        "The slings and arrows of outrageous fortune, ",
        "The slings and arrows of outrageous fortune, ",
        "The slings and arrows of outrageous fortune, ",
        "The slings and arrows of outrageous fortune, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ",
        "Or to take arms against a sea of troubles, ");

    // Create a Java Collection, in this case a List of Strings.
    final List<String> sources = new ArrayList<>();
    int numOfSources = 100;
    for (int i = 0; i < numOfSources; i++) {
      sources.add(String.format("%d", i));
    }


    pipeline
        // .apply(Create.of(lines)).setCoder(StringUtf8Coder.of())
        .apply("Create SDF sources", Create.of(sources)).setCoder(StringUtf8Coder.of())
        .apply("Generate timestamps", ParDo.of(new GenerateRandomStringsDoFn<String, String>()))

        // Group records into 1 minute windows
        // .apply(Window.into(FixedWindows.of(Duration.standardMinutes(10))))

        // .apply("Filter to only records from the Users table", Filter.by(
        //     (SerializableFunction<DataChangeRecord, Boolean>) record -> record.getTableName()
        //         .equalsIgnoreCase("Users")))

        // TODO map the change record mods into Spanner data records
        // TODO handle updates, deletes, and inserts differently in BigQuery
        // .apply("Get data records from Spanner", MapElements.into(TypeDescriptor.of(RecordWithMetadata.class)).via(
        //     (SerializableFunction<RecordWithMetadata, RecordWithMetadata>) new SimpleFunction<RecordWithMetadata, RecordWithMetadata>() {
        //       @Override
        //       public RecordWithMetadata apply(RecordWithMetadata record) {
        //         try (ResultSet resultSet =
        //             databaseClient
        //                 .singleUse(TimestampBound.ofReadTimestamp(record.dataChangeRecord.getCommitTimestamp()))
        //                     .read(
        //                         "Users", KeySet.singleKey(Key.newBuilder().append(record.dataChangeRecord.getMods().get(0).getKeysJson()).build()), Arrays.asList("SingerId", "AlbumId", "MarketingBudget"))) {
        //           while (resultSet.next()) {
        //
        //           }
        //         }
        //       }
        //     }))

        // Writes each window of records into BigQuery
        .apply("Write to BigQuery table",
            BigQueryIO
                .<Long>write()
                .withCustomGcsTempLocation(StaticValueProvider.of(gcsTempLocation))
                .to(bigQueryTable)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                .withSchema(createSchema())
                // .optimizedWrites()
                // .withAutoSharding()
                // .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .withFormatFunction((Long seconds) ->
                    new TableRow()
                        .set(BQ_SCHEMA_NAME_PARTITION_TOKEN, UUID.randomUUID().toString())
                        .set(BQ_SCHEMA_NAME_COMMITTED_TIMESTAMP, seconds)
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
                .setName(BQ_SCHEMA_NAME_COMMITTED_TIMESTAMP)
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
