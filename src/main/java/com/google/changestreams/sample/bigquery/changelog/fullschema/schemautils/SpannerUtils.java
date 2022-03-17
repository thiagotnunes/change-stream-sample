package com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils;

import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerColumn;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerTable;
import com.google.cloud.spanner.*;
import org.json.JSONObject;

import java.util.*;

/**
 * Class {@link SpannerUtils} provides methods that retrieve schema information from Spanner.
 */
public class SpannerUtils {

  private static final String INFORMATION_SCHEMA_TABLE_NAME = "TABLE_NAME";
  private static final String INFORMATION_SCHEMA_COLUMN_NAME = "COLUMN_NAME";
  private static final String INFORMATION_SCHEMA_SPANNER_TYPE = "SPANNER_TYPE";
  private static final String INFORMATION_SCHEMA_ORDINAL_POSITION = "ORDINAL_POSITION";
  private static final String INFORMATION_SCHEMA_CONSTRAINT_NAME = "CONSTRAINT_NAME";
  private static final String INFORMATION_SCHEMA_ALL = "ALL";

  private DatabaseClient databaseClient;
  private String changeStreamName;

  public SpannerUtils(DatabaseClient databaseClient, String changeStreamName) {
    this.databaseClient = databaseClient;
    this.changeStreamName = changeStreamName;
  }

  /**
   * @return a map where the key is the table name and the value is the {@link SpannerTable} object
   * of the table name.
   */
  public Map<String, SpannerTable> getSpannerTableByName() {
    final Set<String> spannerTableNames = getSpannerTableNamesTrackedByChangeStreams();

    final Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName =
      getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName();

    return getSpannerTableByName(spannerTableNames,
      spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);
  }

  private Map<String, SpannerTable> getSpannerTableByName(Set<String> spannerTableNames,
    Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {
    final Map<String, List<SpannerColumn>> spannerColumnsByTableName = getSpannerColumnsByTableName(
      spannerTableNames,
      spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName);
    final Map<String, Set<String>> keyColumnNameByTableName = getKeyColumnNameByTableName(
      spannerTableNames);
    
    final Map<String, SpannerTable> result = new HashMap<>();
    for (final String tableName : spannerColumnsByTableName.keySet()) {
      final List<SpannerColumn> pkColumns = new LinkedList<>();
      final List<SpannerColumn> nonPkColumns = new LinkedList<>();
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
   * Query INFORMATION_SCHEMA.COLUMNS to construct {@link SpannerColumn} for each Spanner column
   * tracked by Change Stream.
   */
  private Map<String, List<SpannerColumn>> getSpannerColumnsByTableName(
    Set<String> spannerTableNames,
    Map<String, Set<String>> spannerColumnNamesExplicitlyTrackedByChangeStreamByTableName) {
    final Map<String, List<SpannerColumn>> result = new HashMap<>();
    final StringBuilder sqlStringBuilder = new StringBuilder(
      "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
        + "FROM INFORMATION_SCHEMA.COLUMNS");

    // Skip the columns of the tables that are not tracked by Change Streams.
    if (!spannerTableNames.isEmpty()) {
      sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
    }

    Statement.Builder statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    if (!spannerTableNames.isEmpty()) {
      statementBuilder.bind("tableNames").to(Value.stringArray(new ArrayList<>(spannerTableNames)));
    }

    try (final ResultSet columnsResultSet =
           databaseClient
             .singleUse()
             .executeQuery(statementBuilder.build())) {
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
        result.putIfAbsent(tableName, new ArrayList<>());
        final SpannerColumn spannerColumn = SpannerColumn.create(columnName,
          informationSchemaTypeToSpannerType(spannerType), ordinalPosition);
        result.get(tableName).add(spannerColumn);
      }
    }

    return result;
  }

  /*
   * Query INFORMATION_SCHEMA.KEY_COLUMN_USAGE to get the names of the primary key columns that are
   * tracked by Change Stream.
   */
  private Map<String, Set<String>> getKeyColumnNameByTableName(
    Set<String> spannerTableNames) {
    final Map<String, Set<String>> result = new HashMap<>();
    final StringBuilder sqlStringBuilder = new StringBuilder(
      "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE");

    // Skip the columns of the tables that are not tracked by Change Streams.
    if (!spannerTableNames.isEmpty()) {
      sqlStringBuilder.append(" WHERE TABLE_NAME IN UNNEST (@tableNames)");
    }

    Statement.Builder statementBuilder = Statement.newBuilder(sqlStringBuilder.toString());
    if (!spannerTableNames.isEmpty()) {
      statementBuilder.bind("tableNames").to(Value.stringArray(new ArrayList<>(spannerTableNames)));
    }

    try (final ResultSet keyColumnsResultSet =
           databaseClient
             .singleUse()
             .executeQuery(statementBuilder.build())) {
      while (keyColumnsResultSet.next()) {
        final String tableName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
        final String columnName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
        final String constraintName = keyColumnsResultSet.getString(INFORMATION_SCHEMA_CONSTRAINT_NAME);
        // We are only interested in primary key constraint.
        if (isPrimaryKey(constraintName)) {
          result.putIfAbsent(tableName, new HashSet<>());
          result.get(tableName).add(columnName);
        }
      }
    }

    return result;
  }

  private static boolean isPrimaryKey(String constraintName) {
    return constraintName.startsWith("PK");
  }

  /**
   * @return the Spanner table names that are tracked by the Change Stream.
   */
  private Set<String> getSpannerTableNamesTrackedByChangeStreams() {
    final boolean isChangeStreamForAll = isChangeStreamForAll();

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

    final Set<String> result = new HashSet<>();
    try (final ResultSet resultSet =
           databaseClient
             .singleUse()
             .executeQuery(statementBuilder.build())) {

      while (resultSet.next()) {
        result.add(resultSet.getString(INFORMATION_SCHEMA_TABLE_NAME));
      }
    }

    return result;
  }

  /**
   * @return if the Change Stream tracks all the tables in the database.
   */
  private boolean isChangeStreamForAll() {
    final String sql =
      "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS " +
        "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    Boolean result = null;
    try (final ResultSet resultSet =
           databaseClient
             .singleUse()
             .executeQuery(
               Statement.newBuilder(sql)
                 .bind("changeStreamName").to(changeStreamName)
                 .build())) {

      while (resultSet.next()) {
        result = resultSet.getBoolean(INFORMATION_SCHEMA_ALL);
      }
    }

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
  private Map<String, Set<String>>
  getSpannerColumnNamesExplicitlyTrackedByChangeStreamsByTableName() {
    final String sql =
      "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
        + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    final Map<String, Set<String>> result = new HashMap<>();
    try (final ResultSet resultSet =
           databaseClient
             .singleUse()
             .executeQuery(
               Statement.newBuilder(sql)
                 .bind("changeStreamName").to(changeStreamName)
                 .build())) {

      while (resultSet.next()) {
        final String tableName = resultSet.getString(INFORMATION_SCHEMA_TABLE_NAME);
        final String columnName = resultSet.getString(INFORMATION_SCHEMA_COLUMN_NAME);
        result.putIfAbsent(tableName, new HashSet<>());
        result.get(tableName).add(columnName);
      }
    }

    return result;
  }

  private Type informationSchemaTypeToSpannerType(String type) {
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

  private String cleanInformationSchemaType(String type) {
    // Remove type size, e.g. STRING(1024) -> STRING.
    final int leftParenthesisIdx = type.indexOf('(');
    if (leftParenthesisIdx != -1) {
      type = type.substring(0, leftParenthesisIdx) + type.substring(type.indexOf(')') + 1);
    }

    // Convert it to upper case.
    return type.toUpperCase();
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
}
