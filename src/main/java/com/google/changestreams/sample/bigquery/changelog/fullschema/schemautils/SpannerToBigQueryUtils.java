package com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerColumn;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Class {@link SpannerToBigQueryUtils} provides methods that convert Spanner types to BigQuery
 * types.
 */
public class SpannerToBigQueryUtils {

  public static List<TableFieldSchema> spannerColumnsToBigQueryIOFields(
    List<SpannerColumn> spannerColumns) {
    final List<TableFieldSchema> bigQueryFields = new ArrayList<>(spannerColumns.size());
    for (SpannerColumn spannerColumn : spannerColumns) {
      bigQueryFields.add(spannerColumnToBigQueryIOField(spannerColumn));
    }

    return bigQueryFields;
  }

  private static TableFieldSchema spannerColumnToBigQueryIOField(SpannerColumn spannerColumn) {
    final TableFieldSchema bigQueryField = new TableFieldSchema()
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
      final StandardSQLTypeName bigQueryType;
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

  public static void spannerSnapshotRowToBigQueryTableRow(
    ResultSet resultSet, List<SpannerColumn> spannerNonPkColumns, TableRow tableRow) {
    // We will only receive one row.
    int rowCount = 0;
    while (resultSet.next()) {
      if (rowCount > 1) {
        throw new IllegalArgumentException(
          "Received more than one row from the result set of Spanner snapshot row");
      }

      for (final SpannerColumn spannerNonPkColumn : spannerNonPkColumns) {
        tableRow.set(spannerNonPkColumn.getName(),
          getColumnValueFromResultSet(spannerNonPkColumn, resultSet));
      }

      rowCount++;
    }
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
      final List<String> result = new LinkedList<>();
      for (final ByteArray bytes : bytesList) {
        // TODO: IS base64 encoding sufficient?
        result.add(bytes.toBase64());
      }
      return result;
    } else if (columnType.equals(Type.array(Type.date()))) {
      final List<String> result = new LinkedList<>();
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
