/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.changestreams.sample.bigquery.changelog.fullschema;

import com.google.api.services.bigquery.model.TableRow;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.Mod;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerColumn;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerTable;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.BigQueryUtils;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.SpannerToBigQueryUtils;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.SpannerUtils;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Key.Builder;
import com.google.cloud.spanner.Options;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class {@link FailsafeModJsonToTableRowTransformer} provides methods that convert a {@link Mod}
 * wrapped in {@link FailsafeElement} to a {@link TableRow}.
 */
public class FailsafeModJsonToTableRowTransformer {
  /**
   * Primary class for taking a {@link FailsafeElement} {@link Mod} JSON input and converting to a
   * {@link TableRow}.
   */
  public static class FailsafeModJsonToTableRow
    extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    /**
     * The tag for the main output of the transformation.
     */
    public TupleTag<TableRow> transformOut = new TupleTag<TableRow>() {
    };

    /**
     * The tag for the dead letter output of the transformation.
     */
    public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut =
      new TupleTag<FailsafeElement<String, String>>() {
      };

    private final String spannerProject;
    private final String spannerInstance;
    private final String spannerDatabase;
    private final String spannerHost;
    private final String changeStreamName;
    private final FailsafeElementCoder<String, String> coder;

    public FailsafeModJsonToTableRow(
      String spannerProject,
      String spannerInstance,
      String spannerDatabase,
      String spannerHost,
      String changeStreamName,
      FailsafeElementCoder<String, String> coder) {
      this.spannerProject = spannerProject;
      this.spannerInstance = spannerInstance;
      this.spannerDatabase = spannerDatabase;
      this.spannerHost = spannerHost;
      this.changeStreamName = changeStreamName;
      this.coder = coder;
    }

    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

      PCollectionTuple out = input.apply(
        ParDo.of(new FailsafeModJsonToTableRowFn(
            spannerProject,
            spannerInstance,
            spannerDatabase,
            spannerHost,
            changeStreamName,
            transformOut,
            transformDeadLetterOut))
          .withOutputTags(transformOut, TupleTagList.of(transformDeadLetterOut)));
      out.get(transformDeadLetterOut).setCoder(this.coder);
      return out;
    }

    public static class FailsafeModJsonToTableRowFn extends
      DoFn<FailsafeElement<String, String>, TableRow> {

      private final String spannerProject;
      private final String spannerInstance;
      private final String spannerDatabase;
      private final String spannerHost;
      private final String changeStreamsName;
      public TupleTag<TableRow> transformOut;
      public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut;
      private Map<String, SpannerTable> spannerTableByName = null;
      private Spanner spanner;
      private DatabaseClient spannerDatabaseClient;

      public FailsafeModJsonToTableRowFn(
        String spannerProject,
        String spannerInstance,
        String spannerDatabase,
        String spannerHost,
        String changeStreamsName,
        TupleTag<TableRow> transformOut,
        TupleTag<FailsafeElement<String, String>> transformDeadLetterOut) {
        this.spannerProject = spannerProject;
        this.spannerInstance = spannerInstance;
        this.spannerDatabase = spannerDatabase;
        this.spannerHost = spannerHost;
        this.changeStreamsName = changeStreamsName;
        this.transformOut = transformOut;
        this.transformDeadLetterOut = transformDeadLetterOut;
      }

      @Setup
      public void setUp() {
        this.spanner = SpannerOptions.newBuilder()
          .setHost(spannerHost)
          .setProjectId(spannerProject)
          .build()
          .getService();
        this.spannerDatabaseClient = spanner.getDatabaseClient(
          DatabaseId.of(spannerProject, spannerInstance, spannerDatabase));

        spannerTableByName = new SpannerUtils(spannerDatabaseClient, changeStreamsName)
          .getSpannerTableByName();
      }

      @Teardown
      public void tearDown() {
        spanner.close();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        FailsafeElement<String, String> failsafeModJsonString = context.element();

        try {
          final TableRow tableRow = modJsonStringToTableRow(failsafeModJsonString.getPayload());
          context.output(tableRow);
        } catch (Exception e) {
          context.output(
            transformDeadLetterOut,
            FailsafeElement.of(failsafeModJsonString)
              .setErrorMessage(e.getMessage())
              .setStacktrace(Throwables.getStackTraceAsString(e)));
        }
      }

      private TableRow modJsonStringToTableRow(String modJsonString) throws Exception {
        final ObjectNode modObjectNode = (ObjectNode) new ObjectMapper().readTree(modJsonString);
        for (final String excludeFieldName : BigQueryUtils.getBigQueryIntermediateMetadataFieldNames()) {
          if (modObjectNode.has(excludeFieldName)) {
            modObjectNode.remove(excludeFieldName);
          }
        }

        final Mod mod = Mod.fromJson(modObjectNode.toString());
        final String spannerTableName = mod.getTableName();
        final SpannerTable spannerTable = spannerTableByName.get(spannerTableName);
        final com.google.cloud.Timestamp spannerCommitTimestamp =
          com.google.cloud.Timestamp
            .ofTimeSecondsAndNanos(mod.getCommitTimestampSeconds(), mod.getCommitTimestampNanos());

        final TableRow tableRow = new TableRow();
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON, modJsonString);
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_MOD_TYPE, mod.getModType().name());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME, spannerTableName);
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP,
          spannerCommitTimestamp.toString());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID,
          mod.getServerTransactionId());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE, mod.getRecordSequence());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION,
          mod.getIsLastRecordInTransactionInPartition());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION,
          mod.getNumberOfRecordsInTransaction());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION,
          mod.getNumberOfPartitionsInTransaction());
        tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP, "AUTO");

        final JSONObject keysJsonObject = new JSONObject(mod.getKeysJson());
        final Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
        for (final SpannerColumn spannerColumn : spannerTable.getPkColumns()) {
          final String spannerColumnName = spannerColumn.getName();
          // TODO: Test the case where some keys are null/empty from Mod.
          if (keysJsonObject.has(spannerColumnName)) {
            tableRow.set(spannerColumnName, keysJsonObject.get(spannerColumnName));
            SpannerUtils.appendToSpannerKey(spannerColumn, keysJsonObject, keyBuilder);
          }
        }

        final List<SpannerColumn> spannerNonPkColumns = spannerTable.getNonPkColumns();
        final List<String> spannerNonPkColumnNames = new ArrayList<>(spannerNonPkColumns.size());
        for (final SpannerColumn spannerNonPkColumn : spannerNonPkColumns) {
          spannerNonPkColumnNames.add(spannerNonPkColumn.getName());
        }

        final Options.ReadQueryUpdateTransactionOption options = Options.priority(
          Options.RpcPriority.HIGH);
        try (final ResultSet resultSet =
          spannerDatabaseClient
            .singleUseReadOnlyTransaction(TimestampBound.ofReadTimestamp(spannerCommitTimestamp))
            .read(
              spannerTable.getTableName(),
              KeySet.singleKey(keyBuilder.build()),
              spannerNonPkColumnNames, options)) {
          SpannerToBigQueryUtils.spannerSnapshotRowToBigQueryTableRow(
            resultSet, spannerNonPkColumns, tableRow);
        }

        return tableRow;
      }
    }
  }
}
