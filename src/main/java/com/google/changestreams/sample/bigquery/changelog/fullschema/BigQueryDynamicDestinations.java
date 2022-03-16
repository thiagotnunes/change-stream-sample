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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerTable;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.BigQueryUtils;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.SpannerToBigQueryUtils;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.SpannerUtils;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.util.List;
import java.util.Map;

/**
 * Class {@link BigQueryDynamicDestinations} loads into BigQuery tables in a dynamic fashion.
 * The destination table is inferred from the provided {@link TableRow}.
 */
public class BigQueryDynamicDestinations extends
  DynamicDestinations<TableRow, KV<TableId, TableRow>> {

  private final Map<String, SpannerTable> spannerTableByName;
  private final String bigQueryProject, bigQueryDataset, bigQueryTableTemplate;

  BigQueryDynamicDestinations(
    String spannerProject,
    String spannerInstance,
    String spannerDatabase,
    String spannerHost,
    String changeStreamName,
    String bigQueryProject,
    String bigQueryDataset,
    String bigQueryTableTemplate) {
    Spanner spanner = SpannerOptions.newBuilder()
      .setHost(spannerHost)
      .setProjectId(spannerProject)
      .build()
      .getService();
    final DatabaseClient spannerDatabaseClient = spanner.getDatabaseClient(
      DatabaseId.of(spannerProject, spannerInstance, spannerDatabase));
    spannerTableByName = new SpannerUtils(spannerDatabaseClient, changeStreamName)
      .getSpannerTableByName();
    spanner.close();
    this.bigQueryProject = bigQueryProject;
    this.bigQueryDataset = bigQueryDataset;
    this.bigQueryTableTemplate = bigQueryTableTemplate;
  }

  private TableId getTableId(String bigQueryTableTemplate, TableRow tableRow) {
    final String bigQueryTableName = BigQueryConverters.formatStringTemplate(
      bigQueryTableTemplate, tableRow);

    return TableId.of(bigQueryProject, bigQueryDataset, bigQueryTableName);
  }

  @Override
  public KV<TableId, TableRow> getDestination(ValueInSingleWindow<TableRow> element) {
    final TableRow tableRow = element.getValue();
    return KV.of(getTableId(bigQueryTableTemplate, tableRow), tableRow);
  }

  @Override
  public TableDestination getTable(KV<TableId, TableRow> destination) {
    final TableId tableId = getTableId(bigQueryTableTemplate, destination.getValue());
    final String tableName =
      String.format("%s:%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());

    return new TableDestination(tableName, "BigQuery changelog table.");
  }

  @Override
  public TableSchema getSchema(KV<TableId, TableRow> destination) {
    final TableRow tableRow = destination.getValue();
    final String spannerTableName = (String) tableRow.get(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME);
    final SpannerTable spannerTable = spannerTableByName.get(spannerTableName);

    final List<TableFieldSchema> fields = SpannerToBigQueryUtils.spannerColumnsToBigQueryIOFields(
      spannerTable.getAllColumns());

    // Add all metadata fields.
    final String requiredMode = Field.Mode.REQUIRED.name();
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_MOD_TYPE)
        .setType(StandardSQLTypeName.STRING.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME)
        .setType(StandardSQLTypeName.STRING.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP)
        .setType(StandardSQLTypeName.TIMESTAMP.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID)
        .setType(StandardSQLTypeName.STRING.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE)
        .setType(StandardSQLTypeName.STRING.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION)
        .setType(StandardSQLTypeName.BOOL.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION)
        .setType(StandardSQLTypeName.INT64.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION)
        .setType(StandardSQLTypeName.INT64.name()).setMode(requiredMode));
    fields
      .add(new TableFieldSchema()
        .setName(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP)
        .setType(StandardSQLTypeName.TIMESTAMP.name()).setMode(requiredMode));

    return new TableSchema().setFields(fields);
  }
}
