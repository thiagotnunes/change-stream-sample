package com.google.changestreams.sample.timestampOrdering;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowOrderedGloballyByKeyCodeSample {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataflowOrderedGloballyByKeyCodeSample.class);

  private static final String BQ_SCHEMA_NAME_KEY_PREFIX = "key_prefix";
  private static final String BQ_SCHEMA_NAME_ORDERED_RECORDS = "ordered_records";
  private static final String BQ_SCHEMA_EARLIEST_RECORD_TIMESTAMP = "earliest_record_timestamp";
  private static final String BQ_SCHEMA_WINDOW_END_TIMESTAMP = "window_end_timestamp";
  private static final String BQ_SCHEMA_EMIT_TIMESTAMP = "emit_timestamp";
  private static final String BQ_SCHEMA_NAME_BQ_TIMESTAMP = "bq_timestamp";

  private static class RecordsOrderedWithinKey implements Serializable {

    public String keyPrefix;
    public String orderedRecords;
    public Timestamp earliestRecordTimestamp;
    public Timestamp windowEndTimestamp;
    public Timestamp emittedTime;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecordsOrderedWithinKey)) {
        return false;
      }
      RecordsOrderedWithinKey that = (RecordsOrderedWithinKey) o;
      return keyPrefix.equals(that.keyPrefix) &&
          orderedRecords.equals(that.orderedRecords) &&
          earliestRecordTimestamp.equals(that.earliestRecordTimestamp) &&
          windowEndTimestamp.equals(that.windowEndTimestamp) &&
          emittedTime.equals(that.emittedTime);
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

    final com.google.cloud.Timestamp inclusiveStartAt = com.google.cloud.Timestamp.now();
    // final com.google.cloud.Timestamp inclusiveEndAt = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
    //     inclusiveStartAt.getSeconds() + (60 * 60),
    //     inclusiveStartAt.getNanos()
    // );
    final long timeIncrementInSeconds = 5;

    // pipeline.apply(GenerateSequence.from(0))
    pipeline
        //Reads from the change stream
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
            // .withInclusiveEndAt(inclusiveEndAt)
        )
        .apply(ParDo.of(new BreakRecordByModFn()))
        .apply(ParDo.of(new KeyByIdFn()))
        .apply(
            ParDo.of(new BufferKeyUntilOutputTimestamp(null, timeIncrementInSeconds)))
        .apply(ParDo.of(new CreateRecordWithMetadata()))

        // Writes each window of records into BigQuery
        .apply("Write to BigQuery table",
            BigQueryIO
                .<RecordsOrderedWithinKey>write()
                .to(bigQueryTable)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
                .withSchema(createSchema())
                .withAutoSharding()
                // Uncomment line below for BigQuery Storage Write API
                // .withMethod(BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE)
                .optimizedWrites()
                .withFormatFunction((RecordsOrderedWithinKey elem) ->
                    new TableRow()
                        .set(BQ_SCHEMA_NAME_KEY_PREFIX, elem.keyPrefix)
                        .set(BQ_SCHEMA_NAME_ORDERED_RECORDS, elem.orderedRecords)
                        .set(BQ_SCHEMA_EARLIEST_RECORD_TIMESTAMP, elem.earliestRecordTimestamp.getSeconds())
                        .set(BQ_SCHEMA_WINDOW_END_TIMESTAMP, elem.windowEndTimestamp.getSeconds())
                        .set(BQ_SCHEMA_EMIT_TIMESTAMP, elem.emittedTime.getSeconds())
                        .set(BQ_SCHEMA_NAME_BQ_TIMESTAMP, "AUTO")
                )
        );

    pipeline.run().waitUntilFinish();
  }

  private static TableSchema createSchema() {
    return new TableSchema().setFields(
        Arrays.asList(
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_KEY_PREFIX)
                .setType("STRING")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_ORDERED_RECORDS)
                .setType("STRING")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_EARLIEST_RECORD_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_WINDOW_END_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_EMIT_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED"),
            new TableFieldSchema()
                .setName(BQ_SCHEMA_NAME_BQ_TIMESTAMP)
                .setType("TIMESTAMP")
                .setMode("REQUIRED")
        )
    );
  }

  // Data change records may contain multiple mods if there are multiple primary keys.
  // Break each data change record into one or more data change records, one per each mod.
  private static class BreakRecordByModFn extends DoFn<DataChangeRecord, DataChangeRecord> {
    private static final long serialVersionUID = 543765692722095666L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record, OutputReceiver<DataChangeRecord> outputReceiver) {
      record.getMods().stream()
          .map(
              mod ->
                  new DataChangeRecord(
                      record.getPartitionToken(),
                      record.getCommitTimestamp(),
                      record.getServerTransactionId(),
                      record.isLastRecordInTransactionInPartition(),
                      record.getRecordSequence(),
                      record.getTableName(),
                      record.getRowType(),
                      Collections.singletonList(mod),
                      record.getModType(),
                      record.getValueCaptureType(),
                      record.getNumberOfRecordsInTransaction(),
                      record.getNumberOfPartitionsInTransaction(),
                      record.getMetadata()))
          .forEach(outputReceiver::output);
    }
  }

  // Key the data change record by its JSON key string.
  private static class KeyByIdFn extends DoFn<DataChangeRecord, KV<String, DataChangeRecord>> {
    private static final long serialVersionUID = 5121794565566403405L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record,
        OutputReceiver<KV<String, DataChangeRecord>> outputReceiver) {
      // Get the key prefix
      String keyString = record.getMods().get(0).getKeysJson();
      String keySubstring = keyString.substring(1, keyString.length() - 1);
      String[] arrayOfKeys = keySubstring.split(",", -1);
      String keyPrefix = arrayOfKeys[0];
      outputReceiver.output(KV.of(keyPrefix, record));
    }
  }

  // Timers and buffers are per-key.
  // Buffer each data change record until the watermark passes the timestamp at which we want
  // to output the buffered data change records.
  // We utilize a looping timer to determine when to flush the buffer:
  //
  // 1. When we see a data change record for a key for the first time, we will set the timer to
  //    fire at the data change record's commit timestamp.
  // 2. Then, when the timer fires, if we have data that whose timestamp is greater than or equal
  //    to the timer's expiration time, we will add that data back to the buffer instead of
  //    outputting it. We will then set the next timer to the current timer's expiration time
  //    plus incrementIntervalInSeconds.
  // 3. If we have data to output, we will order the data change records by commit timestamp and
  //    transaction ID before outputting it.
  //
  private static class BufferKeyUntilOutputTimestamp
      extends DoFn<KV<String, DataChangeRecord>, KV<String, Iterable<DataChangeRecord>>> {
    private static final long serialVersionUID = 5050535558953049259L;

    private final long incrementIntervalInSeconds;
    private final @Nullable Instant pipelineEndTime;

    private BufferKeyUntilOutputTimestamp(
        @Nullable com.google.cloud.Timestamp endTimestamp, long incrementIntervalInSeconds) {
      this.incrementIntervalInSeconds = incrementIntervalInSeconds;
      if (endTimestamp != null) {
        this.pipelineEndTime = new Instant(endTimestamp.toSqlTimestamp());
      } else {
        pipelineEndTime = null;
      }
    }

    @SuppressWarnings("unused")
    @TimerId("timer")
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @SuppressWarnings("unused")
    @StateId("buffer")
    private final StateSpec<BagState<DataChangeRecord>> buffer = StateSpecs.bag();

    @SuppressWarnings("unused")
    @StateId("keySeen")
    private final StateSpec<ValueState<Boolean>> keySeen = StateSpecs.value(BooleanCoder.of());

    @ProcessElement
    public void process(
        @Element KV<String, DataChangeRecord> element,
        @StateId("buffer") BagState<DataChangeRecord> buffer,
        @TimerId("timer") Timer timer,
        @StateId("keySeen") ValueState<Boolean> keySeen) {
      buffer.add(element.getValue());

      // Only set the timer if this is the first time we are receiving a data change record
      // with this key.
      Boolean hasKeyBeenSeen = keySeen.read();
      if (hasKeyBeenSeen == null) {
        Instant commitTimestamp =
            new Instant(element.getValue().getCommitTimestamp().toSqlTimestamp());
        Instant outputTimestamp =
            commitTimestamp.plus(Duration.standardSeconds(incrementIntervalInSeconds));
        LOG.info("Setting timer at {} for key {}", outputTimestamp.toString(), element.getKey());
        timer.set(outputTimestamp);
        keySeen.write(true);
      }
    }

    @OnTimer("timer")
    public void onExpiry(
        OnTimerContext context,
        @StateId("buffer") BagState<DataChangeRecord> buffer,
        @TimerId("timer") Timer timer,
        @StateId("keySeen") ValueState<Boolean> keySeen) {
      if (!buffer.isEmpty().read()) {
        final List<DataChangeRecord> records =
            StreamSupport.stream(buffer.read().spliterator(), false)
                .collect(Collectors.toList());
        buffer.clear();

        List<DataChangeRecord> recordsToOutput = new ArrayList<>();
        for (DataChangeRecord record : records) {
          Instant recordCommitTimestamp =
              new Instant(record.getCommitTimestamp().toSqlTimestamp());
          final String recordString =
              record.getMods().get(0).getNewValuesJson().isEmpty()
                  ? "Deleted record"
                  : record.getMods().get(0).getNewValuesJson();
          // When the watermark passes time T, this means that all records with event time < T
          // have been processed and successfully committed. Since the timer fires when the
          // watermark passes the expiration time, we should only output records with event time
          // < expiration time.
          if (recordCommitTimestamp.isBefore(context.timestamp())) {
            LOG.info(
                "Outputting record with key {} and value \"{}\" at expiration timestamp {}",
                record.getMods().get(0).getKeysJson(),
                recordString,
                context.timestamp().toString());
            recordsToOutput.add(record);
          } else {
            LOG.info(
                "Expired at {} but adding record with key {} and value {} back to buffer "
                    + "due to commit timestamp {}",
                context.timestamp().toString(),
                record.getMods().get(0).getKeysJson(),
                recordString,
                recordCommitTimestamp.toString());
            buffer.add(record);
          }
        }

        // Output records, if there are any to output.
        if (!recordsToOutput.isEmpty()) {
          // Order the records in place, and output them.
          Collections.sort(recordsToOutput, new DataChangeRecordComparator());
          context.outputWithTimestamp(
              KV.of(recordsToOutput.get(0).getMods().get(0).getKeysJson(),
                  recordsToOutput), context.timestamp());
          LOG.info(
              "Expired at {}, outputting records for key {}",
              context.timestamp().toString(),
              recordsToOutput.get(0).getMods().get(0).getKeysJson());
        } else {
          LOG.info("Expired at {} with no records", context.timestamp().toString());
        }
      }

      Instant nextTimer =
          context.timestamp().plus(Duration.standardSeconds(incrementIntervalInSeconds));
      if (buffer.isEmpty() != null && !buffer.isEmpty().read()) {
        LOG.info("Setting next timer to {}", nextTimer.toString());
        timer.set(nextTimer);
      } else {
        LOG.info(
            "Timer not being set since the buffer is empty: ");
        keySeen.clear();
        LOG.info("Wrote keyseen");
      }
    }
  }

  // Output a string representation of the iterable of data change records ordered by commit
  // timestamp and record sequence per key.
  private static class CreateRecordWithMetadata
      extends DoFn<KV<String, Iterable<DataChangeRecord>>, RecordsOrderedWithinKey> {
    private static final long serialVersionUID = -2573561902102768101L;

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<DataChangeRecord>> recordsByKey,
        @Timestamp Instant elementTimestamp,
        OutputReceiver<RecordsOrderedWithinKey> outputReceiver) {
      final List<DataChangeRecord> sortedRecords =
          StreamSupport.stream(recordsByKey.getValue().spliterator(), false)
              .collect(Collectors.toList());

      // Get the earliest record timestamp.
      final com.google.cloud.Timestamp earliestTimestamp = sortedRecords.get(0).getCommitTimestamp();

      // Get the key prefix.
      String keyString = sortedRecords.get(0).getMods().get(0).getKeysJson();
      String keySubstring = keyString.substring(1, keyString.length() - 1);
      String[] arrayOfKeys = keySubstring.split(",", -1);
      final String keyPrefix = arrayOfKeys[0];

      // Get the record string.
      final StringBuilder builder = new StringBuilder();
      for (DataChangeRecord record : sortedRecords) {
        builder.append(record.getMods().get(0).getKeysJson() + ",");
        builder.append(record.getCommitTimestamp() + "\n");
      }
      final String recordString = builder.toString();
      
      // Build the records ordered by key prefix.
      RecordsOrderedWithinKey records = new RecordsOrderedWithinKey();
      records.keyPrefix = keyPrefix;
      records.earliestRecordTimestamp = earliestTimestamp;
      records.windowEndTimestamp = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
          elementTimestamp.getMillis() / 1000, 0);
      records.orderedRecords = recordString;
      records.emittedTime = com.google.cloud.Timestamp.now();
      outputReceiver.output(records);
    }
  }

  public static class DataChangeRecordComparator implements Comparator<DataChangeRecord> {
    @Override
    public int compare(DataChangeRecord x, DataChangeRecord y) {
      // Compare commit timestamp
      int timestampComparison = compare(x.getCommitTimestamp(), y.getCommitTimestamp());
      if (timestampComparison != 0) {
        return timestampComparison;
      }

      // Compare transaction ID.
      int transactionIdComparison = x.getServerTransactionId().compareTo(y.getServerTransactionId());
      if (transactionIdComparison != 0) {
        return transactionIdComparison;
      }

      // Compare record sequence.
      return x.getRecordSequence().compareTo(y.getRecordSequence());
    }

    // Compare timestamp.
    private static int compare(com.google.cloud.Timestamp a, com.google.cloud.Timestamp b) {
      double aFractionalSeconds = a.getSeconds() + a.getNanos() / 1000000000.0;
      double bFractionalSeconds = b.getSeconds() + b.getNanos() / 1000000000.0;
      return aFractionalSeconds < bFractionalSeconds ? -1 :
          aFractionalSeconds > bFractionalSeconds ? 1 : 0;
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

