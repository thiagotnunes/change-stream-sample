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
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerColumn;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerTable;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spanner.*;
import org.json.JSONObject;

import java.util.*;

public class SchemaUtils {

  public static final String BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON =
    "_metadata_spanner_original_payload_json";
  public static final String BQ_CHANGELOG_FIELD_NAME_ERROR = "_metadata_error";
  public static final String BQ_CHANGELOG_FIELD_NAME_RETRY_COUNT = "_metadata_retry_count";

  public static final String BQ_CHANGELOG_FIELD_NAME_MOD_TYPE = "_metadata_spanner_mod_type";
  public static final String BQ_CHANGELOG_FIELD_NAME_TABLE_NAME = "_metadata_spanner_table_name";
  public static final String BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP =
    "_metadata_spanner_commit_timestamp";
  public static final String BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID =
    "_metadata_spanner_server_transaction_id";
  public static final String BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE =
    "_metadata_spanner_record_sequence";
  public static final String BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION =
    "_metadata_spanner_is_last_record_in_transaction_in_partition";
  public static final String BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION =
    "_metadata_spanner_number_of_records_in_transaction";
  public static final String BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION =
    "_metadata_spanner_number_of_partitions_in_transaction";
  public static final String BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP =
    "_metadata_big_query_commit_timestamp";

  private static final String INFORMATION_SCHEMA_TABLE_NAME = "TABLE_NAME";
  private static final String INFORMATION_SCHEMA_COLUMN_NAME = "COLUMN_NAME";
  private static final String INFORMATION_SCHEMA_SPANNER_TYPE = "SPANNER_TYPE";
  private static final String INFORMATION_SCHEMA_ORDINAL_POSITION = "ORDINAL_POSITION";
  private static final String INFORMATION_SCHEMA_CONSTRAINT_NAME = "CONSTRAINT_NAME";
  private static final String INFORMATION_SCHEMA_ALL = "ALL";

  /**
   * @return the fields that are only used intermediately in the pipeline and are not corresponding
   * to Spanner columns.
   */
  public static Set<String> getBigQueryIntermediateMetadataFieldNames() {
    Set<String> fieldNames = new HashSet<>();
    fieldNames.add(BQ_CHANGELOG_FIELD_NAME_ERROR);
    fieldNames.add(BQ_CHANGELOG_FIELD_NAME_RETRY_COUNT);
    fieldNames.add(BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON);
    return fieldNames;
  }

  /**
   * @return a map where the key is the table name and the value is the SpannerTable object of the
   * table name.
   */
  public static Map<String, SpannerTable> getSpannerTableByName(
    DatabaseClient databaseClient, String changeStreamName) {
    final Set<String> spannerTableNames = getSpannerTableNamesTrackedByChangeStreams(
      databaseClient, changeStreamName);

    final Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName =
      getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName(
        databaseClient, changeStreamName);

    return getSpannerTableByName(databaseClient, spannerTableNames,
      spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);
  }

  private static void appendWhereTableNamesEqualToSql(
    StringBuilder stringBuilder, Set<String> spannerTableNames) {
    if (!spannerTableNames.isEmpty()) {
      stringBuilder.append(" WHERE");

      for (String tableName : spannerTableNames) {
        stringBuilder.append(" TABLE_NAME=\"");
        stringBuilder.append(tableName);
        stringBuilder.append("\"");
        stringBuilder.append(" OR");
      }

      // Remove the last " OR".
      stringBuilder.setLength(stringBuilder.length() - 3);
    }
  }

  private static Map<String, SpannerTable> getSpannerTableByName(
    DatabaseClient databaseClient, Set<String> spannerTableNames,
    Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {

    StringBuilder queryInformationSchemaColumnsSqlStringBuilder = new StringBuilder(
      "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
        + "FROM INFORMATION_SCHEMA.COLUMNS");

    // Skip the columns of the tables that are not tracked by Change Streams.
    appendWhereTableNamesEqualToSql(queryInformationSchemaColumnsSqlStringBuilder,
      spannerTableNames);

    final ResultSet columnsResultSet =
      databaseClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(queryInformationSchemaColumnsSqlStringBuilder.toString())
            .build());
    Map<String, List<SpannerColumn>> spannerColumnsByTableName = new HashMap<>();
    while (columnsResultSet.next()) {
      final String tableName = columnsResultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
      final String columnName = columnsResultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
      // Skip if the columns of the table is tracked explicitly, and the specified column is not
      // tracked.
      if (spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName.containsKey(tableName)
        && !spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName
        .get(tableName).contains(columnName)) {
        continue;
      }

      final int ordinalPosition = (int) columnsResultSet.getLong(INFORMATION_SCHEMA_ORDINAL_POSITION);
      final String spannerType = columnsResultSet.getString(INFORMATION_SCHEMA_SPANNER_TYPE);
      spannerColumnsByTableName.putIfAbsent(tableName, new ArrayList<>());
      final SpannerColumn spannerColumn = SpannerColumn.create(columnName,
        informationSchemaTypeToSpannerType(spannerType), ordinalPosition);
      spannerColumnsByTableName.get(tableName).add(spannerColumn);
    }

    StringBuilder queryInformationSchemaKeyColumnsSqlStringBuilder = new StringBuilder(
      "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE");
    // Skip the columns of the tables that are not tracked by Change Streams.
    appendWhereTableNamesEqualToSql(queryInformationSchemaKeyColumnsSqlStringBuilder,
      spannerTableNames);

    final ResultSet keyColumnsResultSet =
      databaseClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(queryInformationSchemaKeyColumnsSqlStringBuilder.toString())
            .build());

    Map<String, Set<String>> keyColumnNameByTableName = new HashMap<>();
    while (keyColumnsResultSet.next()) {
      final String tableName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
      final String columnName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
      final String constraintName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_CONSTRAINT_NAME);
      // We are only interested in primary key constraint.
      if (constraintName.startsWith("PK")) {
        keyColumnNameByTableName.putIfAbsent(tableName, new HashSet<>());
        keyColumnNameByTableName.get(tableName).add(columnName);
      }
    }

    Map<String, SpannerTable> result = new HashMap<>();
    for (final String tableName : spannerColumnsByTableName.keySet()) {
      List<SpannerColumn> pkColumns = new LinkedList<>();
      List<SpannerColumn> nonPkColumns = new LinkedList<>();
      final Set<String> keyColumnNames = keyColumnNameByTableName.get(tableName);
      for (final SpannerColumn spannerColumn : spannerColumnsByTableName.get(tableName)) {
        if (keyColumnNames.contains(spannerColumn.getName())) {
          pkColumns.add(spannerColumn);
        } else {
          nonPkColumns.add(spannerColumn);
        }
      }
      result.put(tableName, new SpannerTable(tableName, pkColumns, nonPkColumns));
    }

    return result;
  }

  /**
   * @return the Spanner table names that are tracked by the Change Streams.
   */
  private static Set<String> getSpannerTableNamesTrackedByChangeStreams(
    DatabaseClient databaseClient, String changeStreamName) {
    final boolean isChangeStreamForAll = isChangeStreamForAll(databaseClient, changeStreamName);

    String sql =
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES " +
        "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    Statement.Builder statementBuilder = Statement.newBuilder(sql)
      .bind("changeStreamName").to(changeStreamName);

    if (isChangeStreamForAll) {
      // If the Change Stream is tracking all tables, we have to look up the table names in
      // INFORMATION_SCHEMA.TABLES.
      sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";
      statementBuilder = Statement.newBuilder(sql);
    }

    final ResultSet resultSet =
      databaseClient
        .singleUse()
        .executeQuery(statementBuilder.build());

    Set<String> result = new HashSet<>();
    while (resultSet.next()) {
      result.add(resultSet.getString(INFORMATION_SCHEMA_TABLE_NAME));
    }
    resultSet.close();

    return result;
  }

  /**
   * @return if the Change Stream is tracking all the tables in the database.
   */
  public static boolean isChangeStreamForAll(DatabaseClient databaseClient,
                                               String changeStreamName) {
    final String sql =
      "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS " +
        "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    final ResultSet resultSet =
      databaseClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(sql)
            .bind("changeStreamName").to(changeStreamName)
            .build());

    Boolean result = null;
    while (resultSet.next()) {
      if (resultSet.getBoolean(INFORMATION_SCHEMA_ALL)) {
        result = true;
      } else {
        result = false;
      }
    }
    resultSet.close();

    if (result == null) {
      throw new IllegalArgumentException(
        String.format("Cannot find change stream %s in INFORMATION_SCHEMA", changeStreamName));
    }

    return result;
  }

  /**
   * @return the Spanner column names that are tracked explicitly by Change Streams by table name.
   * e.g. Given the following table:
   * CREATE TABLE Singers (
   * SingerId   INT64 NOT NULL,
   * FirstName  STRING(1024),
   * LastName   STRING(1024),
   * );
   * <p>
   * Return an empty map if we have the following Change Streams:
   * CREATE CHANGE STREAM AllStream FOR ALL
   * <p>
   * Return {"Singers" -> {"SingerId", "LastName"}} if we have the following Change Streams:
   * CREATE CHANGE STREAM SingerStream FOR Singers(SingerId, FirstName)
   */
  private static Map<String, Set<String>>
  getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName(
    DatabaseClient databaseClient, String changeStreamsName) {
    final String sql =
      "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
        + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    final ResultSet resultSet =
      databaseClient
        .singleUse()
        .executeQuery(
          Statement.newBuilder(sql)
            .bind("changeStreamName").to(changeStreamsName)
            .build());

    Map<String, Set<String>> result = new HashMap<>();
    while (resultSet.next()) {
      final String tableName = resultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
      final String columnName = resultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
      result.putIfAbsent(tableName, new HashSet<>());
      result.get(tableName).add(columnName);
    }

    return result;
  }

  private static Type informationSchemaTypeToSpannerType(String type) {
    type = cleanInformationSchemaType(type);
    switch (type) {
      case "BOOL":
        return Type.bool();
      case "BYTES":
        return Type.bytes();
      case "DATE":
        return Type.date();
      case "FLOAT64":
        return Type.float64();
      case "INT64":
        return Type.int64();
      case "JSON":
        return Type.json();
      case "NUMERIC":
        return Type.numeric();
      case "STRING":
        return Type.string();
      case "TIMESTAMP":
        return Type.timestamp();
      default:
        if (type.startsWith("ARRAY")) {
          // Get array type, e.g. "ARRAY<STRING>" -> "STRING".
          final String spannerArrayType = type.substring(6, type.length() - 1);
          final Type itemType = informationSchemaTypeToSpannerType(spannerArrayType);
          return Type.array(itemType);
        }

        throw new IllegalArgumentException(
          String.format("Unsupported Spanner type: %s", type));
    }
  }

  private static String cleanInformationSchemaType(String type) {
    // Remove type size, e.g. STRING(1024) -> STRING.
    final int leftParenthesisIdx = type.indexOf('(');
    if (leftParenthesisIdx != -1) {
      type = type.substring(0, leftParenthesisIdx) + type.substring(type.indexOf(')') + 1);
    }

    // Convert it to upper case.
    return type.toUpperCase();
  }

  public static List<TableFieldSchema> spannerColumnsToBigQueryIOFields(
    List<SpannerColumn> spannerColumns) {
    ArrayList<TableFieldSchema> bigQueryFields = new ArrayList<>(spannerColumns.size());
    for (SpannerColumn spannerColumn : spannerColumns) {
      bigQueryFields.add(spannerColumnToBigQueryIOField(spannerColumn));
    }

    return bigQueryFields;
  }

  private static TableFieldSchema spannerColumnToBigQueryIOField(SpannerColumn spannerColumn) {
    TableFieldSchema bigQueryField = new TableFieldSchema()
      .setName(spannerColumn.getName())
      .setMode(Field.Mode.REPEATED.name());
    final Type spannerType = spannerColumn.getType();

    if (spannerType.equals(Type.array(Type.bool()))) {
      bigQueryField.setType("BOOL");
    } else if (spannerType.equals(Type.array(Type.bytes()))) {
      bigQueryField.setType("BYTES");
    } else if (spannerType.equals(Type.array(Type.date()))) {
      bigQueryField.setType("DATE");
    } else if (spannerType.equals(Type.array(Type.float64()))) {
      bigQueryField.setType("FLOAT64");
    } else if (spannerType.equals(Type.array(Type.int64()))) {
      bigQueryField.setType("INT64");
    } else if (spannerType.equals(Type.array(Type.json()))) {
      bigQueryField.setType("STRING");
    } else if (spannerType.equals(Type.array(Type.numeric()))) {
      bigQueryField.setType("NUMERIC");
    } else if (spannerType.equals(Type.array(Type.string()))) {
      bigQueryField.setType("STRING");
    } else if (spannerType.equals(Type.array(Type.timestamp()))) {
      bigQueryField.setType("TIMESTAMP");
    } else {
      bigQueryField.setMode(Field.Mode.NULLABLE.name());
      StandardSQLTypeName bigQueryType;
      switch (spannerType.getCode()) {
        case BOOL:
          bigQueryType = StandardSQLTypeName.BOOL;
          break;
        case BYTES:
          bigQueryType = StandardSQLTypeName.BYTES;
          break;
        case DATE:
          bigQueryType = StandardSQLTypeName.DATE;
          break;
        case FLOAT64:
          bigQueryType = StandardSQLTypeName.FLOAT64;
          break;
        case INT64:
          bigQueryType = StandardSQLTypeName.INT64;
          break;
        case JSON:
          bigQueryType = StandardSQLTypeName.STRING;
          break;
        case NUMERIC:
          bigQueryType = StandardSQLTypeName.NUMERIC;
          break;
        case STRING:
          bigQueryType = StandardSQLTypeName.STRING;
          break;
        case TIMESTAMP:
          bigQueryType = StandardSQLTypeName.TIMESTAMP;
          break;
        default:
          throw new IllegalArgumentException(
            String.format("Unsupported Spanner type: %s", spannerType));
      }
      bigQueryField.setType(bigQueryType.name());
    }

    return bigQueryField;
  }

  public static void appendToSpannerKey(
    SpannerColumn column, JSONObject keysJsonObject, Key.Builder keyBuilder) {
    final Type.Code code = column.getType().getCode();
    final String name = column.getName();
    switch (code) {
      case BOOL:
        keyBuilder.append(keysJsonObject.getBoolean(name));
        break;
      case FLOAT64:
        keyBuilder.append(keysJsonObject.getDouble(name));
        break;
      case INT64:
        keyBuilder.append(keysJsonObject.getLong(name));
        break;
      case NUMERIC:
        keyBuilder.append(keysJsonObject.getBigDecimal(name));
        break;
      case BYTES:
      case DATE:
      case STRING:
      case TIMESTAMP:
        keyBuilder.append(keysJsonObject.getString(name));
        break;
      default:
        throw new IllegalArgumentException(
          String.format("Unsupported Spanner type: %s", code));
    }
  }

  public static void spannerSnapshotRowToBigQueryTableRow(
    ResultSet resultSet, List<SpannerColumn> spannerNonPkColumns, TableRow tableRow) {
    // We will only receive one row.
    int rowCount = 0;
    while (resultSet.next()) {
      if (rowCount > 1) {
        throw new IllegalArgumentException(
          String.format("Received more than one row from the result set of Spanner snapshot row"));
      }

      for (final SpannerColumn spannerNonPkColumn : spannerNonPkColumns) {
        tableRow.set(spannerNonPkColumn.getName(),
          getColumnValueFromResultSet(spannerNonPkColumn, resultSet));
      }

      rowCount++;
    }

    resultSet.close();
  }

  private static Object getColumnValueFromResultSet(
    SpannerColumn spannerColumn, ResultSet resultSet) {
    final String columnName = spannerColumn.getName();
    final Type columnType = spannerColumn.getType();

    if (resultSet.isNull(columnName)) {
      return null;
    }

    if (columnType.equals(Type.array(Type.bool()))) {
      return resultSet.getBooleanList(columnName);
    } else if (columnType.equals(Type.array(Type.bytes()))) {
      final List<ByteArray> bytesList = resultSet.getBytesList(columnName);
      List<String> result = new LinkedList<>();
      for (final ByteArray bytes : bytesList) {
        // TODO: IS base64 encoding sufficient?
        result.add(bytes.toBase64());
      }
      return result;
    } else if (columnType.equals(Type.array(Type.date()))) {
      List<String> result = new LinkedList<>();
      for (final Date date : resultSet.getDateList(columnName)) {
        result.add(date.toString());
      }
      return result;
    } else if (columnType.equals(Type.array(Type.float64()))) {
      return resultSet.getDoubleList(columnName);
    } else if (columnType.equals(Type.array(Type.int64()))) {
      return resultSet.getLongList(columnName);
    } else if (columnType.equals(Type.array(Type.json()))) {
      return resultSet.getJsonList(columnName);
    } else if (columnType.equals(Type.array(Type.numeric()))) {
      return resultSet.getBigDecimalList(columnName);
    } else if (columnType.equals(Type.array(Type.string()))) {
      return resultSet.getStringList(columnName);
    } else if (columnType.equals(Type.array(Type.timestamp()))) {
      List<String> result = new LinkedList<>();
      for (Timestamp timestamp : resultSet.getTimestampList(columnName)) {
        result.add(timestamp.toString());
      }
      return result;
    } else {
      final Type.Code columnTypeCode = columnType.getCode();
      switch (columnTypeCode) {
        case BOOL:
          return resultSet.getBoolean(columnName);
        case BYTES:
          // TODO: IS base64 encoding sufficient?
          return resultSet.getBytes(columnName).toBase64();
        case DATE:
          return resultSet.getDate(columnName).toString();
        case FLOAT64:
          return resultSet.getDouble(columnName);
        case INT64:
          return resultSet.getLong(columnName);
        case JSON:
          return resultSet.getJson(columnName);
        case NUMERIC:
          return resultSet.getBigDecimal(columnName);
        case STRING:
          return resultSet.getString(columnName);
        case TIMESTAMP:
          return resultSet.getTimestamp(columnName).toString();
        default:
          throw new IllegalArgumentException(
            String.format("Unsupported Spanner type: %s", columnTypeCode));
      }
    }
  }
}
