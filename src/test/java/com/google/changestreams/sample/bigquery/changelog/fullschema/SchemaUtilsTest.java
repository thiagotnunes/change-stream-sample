package com.google.changestreams.sample.bigquery.changelog.fullschema;

import com.google.api.services.bigquery.model.TableRow;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerColumn;
import com.google.changestreams.sample.bigquery.changelog.fullschema.model.SpannerTable;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.SpannerToBigQueryUtils;
import com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils.SpannerUtils;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class SchemaUtilsTest {

  final String changeStreamName = "changeStreamName";
  private DatabaseClient mockDatabaseClient;
  private ReadContext mockReadContext;
  List<SpannerColumn> spannerColumnsOfAllTypes;

  @Before
  public void setUp() {
    mockDatabaseClient = Mockito.mock(DatabaseClient.class);
    mockReadContext = Mockito.mock(ReadContext.class);
    when(mockDatabaseClient.singleUse()).thenReturn(mockReadContext);
    spannerColumnsOfAllTypes = Arrays.asList(
      SpannerColumn.create("BoolCol", Type.bool(), 1),
      SpannerColumn.create("BytesCol", Type.bytes(), 2),
      SpannerColumn.create("DateCol", Type.date(), 3),
      SpannerColumn.create("Float64Col", Type.float64(), 3),
      SpannerColumn.create("Int64Col", Type.int64(), 4),
      SpannerColumn.create("JsonCol", Type.json(), 5),
      SpannerColumn.create("NumericCol", Type.numeric(), 6),
      SpannerColumn.create("StringCol", Type.string(), 7),
      SpannerColumn.create("TimestampCol", Type.timestamp(), 8),
      SpannerColumn.create("BoolArrayCol", Type.array(Type.bool()), 9),
      SpannerColumn.create("BytesArrayCol", Type.array(Type.bytes()), 10),
      SpannerColumn.create("DateArrayCol", Type.array(Type.date()), 11),
      SpannerColumn.create("Float64ArrayCol", Type.array(Type.float64()), 12),
      SpannerColumn.create("Int64ArrayCol", Type.array(Type.int64()), 13),
      SpannerColumn.create("JsonArrayCol", Type.array(Type.json()), 14),
      SpannerColumn.create("NumericArrayCol", Type.array(Type.numeric()), 15),
      SpannerColumn.create("StringArrayCol", Type.array(Type.string()), 16),
      SpannerColumn.create("TimestampArrayCol", Type.array(Type.timestamp()), 17));
  }

  private void mockInformationSchemaChangeStreamsQuery(boolean isTrackingAll) {
    final String sql =
      "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS " +
        "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    when(mockReadContext.executeQuery(
      Statement.newBuilder(sql)
        .bind("changeStreamName").to(changeStreamName)
        .build())).thenReturn(
      ResultSets.forRows(
        Type.struct(Type.StructField.of("ALL", Type.bool())),
        Collections.singletonList(
          Struct.newBuilder().set("ALL").to(Value.bool(isTrackingAll)).build())));
  }

  private void mockInformationSchemaTablesQuery() {
    final String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";

    when(mockReadContext.executeQuery(
      Statement.of(sql))).thenReturn(
      ResultSets.forRows(Type.struct(
          Type.StructField.of("TABLE_NAME", Type.string())),
        Arrays.asList(
          Struct.newBuilder().set("TABLE_NAME").to(Value.string("Singers")).build(),
          Struct.newBuilder().set("TABLE_NAME").to(Value.string("Albums")).build())));
  }

  private void mockInformationSchemaColumnsQuery(boolean isAlbumsTableIncluded) {
    String sql =
      "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
        + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=\"Singers\"";
    List<Struct> rows = new ArrayList<>(Arrays.asList(
      Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Singers"))
        .set("COLUMN_NAME").to(Value.string("SingerId"))
        .set("ORDINAL_POSITION").to(Value.int64(1))
        .set("SPANNER_TYPE").to(Value.string("INT64"))
        .build(),
      Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Singers"))
        .set("COLUMN_NAME").to(Value.string("FirstName"))
        .set("ORDINAL_POSITION").to(Value.int64(2))
        .set("SPANNER_TYPE").to(Value.string("STRING"))
        .build(),
      Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Singers"))
        .set("COLUMN_NAME").to(Value.string("LastName"))
        .set("ORDINAL_POSITION").to(Value.int64(3))
        .set("SPANNER_TYPE").to(Value.string("STRING"))
        .build()));

    if (isAlbumsTableIncluded) {
      sql += " OR TABLE_NAME=\"Albums\"";
      rows.add(Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Albums"))
        .set("COLUMN_NAME").to(Value.string("SingerId"))
        .set("ORDINAL_POSITION").to(Value.int64(1))
        .set("SPANNER_TYPE").to(Value.string("INT64"))
        .build());
      rows.add(Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Albums"))
        .set("COLUMN_NAME").to(Value.string("AlbumId"))
        .set("ORDINAL_POSITION").to(Value.int64(2))
        .set("SPANNER_TYPE").to(Value.string("INT64"))
        .build());
      rows.add(Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Albums"))
        .set("COLUMN_NAME").to(Value.string("AlbumName"))
        .set("ORDINAL_POSITION").to(Value.int64(3))
        .set("SPANNER_TYPE").to(Value.string("STRING"))
        .build());
    }

    when(mockReadContext.executeQuery(
      Statement.of(sql))).thenReturn(
      ResultSets.forRows(Type.struct(
          Type.StructField.of("TABLE_NAME", Type.string()),
          Type.StructField.of("COLUMN_NAME", Type.string()),
          Type.StructField.of("ORDINAL_POSITION", Type.int64()),
          Type.StructField.of("SPANNER_TYPE", Type.string())),
        rows
      ));
  }

  private void mockInformationSchemaChangeStreamTablesQuery() {
    final String sql =
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES " +
        "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    when(mockReadContext.executeQuery(
      Statement.newBuilder(sql)
        .bind("changeStreamName").to(changeStreamName).build()))
      .thenReturn(
        ResultSets.forRows(Type.struct(
            Type.StructField.of("TABLE_NAME", Type.string())),
          Collections.singletonList(
            Struct.newBuilder().set("TABLE_NAME").to(Value.string("Singers")).build())));
  }

  private void mockInformationSchemaKeyColumnUsageQuery(boolean isAlbumsTableIncluded) {
    String sql =
      "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
        "WHERE TABLE_NAME=\"Singers\"";
    List<Struct> rows = new ArrayList<>(Collections.singletonList(
      Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Singers"))
        .set("COLUMN_NAME").to(Value.string("SingerId"))
        .set("CONSTRAINT_NAME").to(Value.string("PK_Singers"))
        .build()
    ));

    if (isAlbumsTableIncluded) {
      sql += " OR TABLE_NAME=\"Albums\"";
      rows.add(Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Albums"))
        .set("COLUMN_NAME").to(Value.string("SingerId"))
        .set("CONSTRAINT_NAME").to(Value.string("PK_Albums"))
        .build());
      rows.add(Struct.newBuilder()
        .set("TABLE_NAME").to(Value.string("Albums"))
        .set("COLUMN_NAME").to(Value.string("AlbumId"))
        .set("CONSTRAINT_NAME").to(Value.string("PK_Albums"))
        .build());
    }

    when(mockReadContext.executeQuery(
      Statement.of(sql))).thenReturn(
      ResultSets.forRows(Type.struct(
        Type.StructField.of("TABLE_NAME", Type.string()),
        Type.StructField.of("COLUMN_NAME", Type.string()),
        Type.StructField.of("CONSTRAINT_NAME", Type.string())),
        rows));
  }

  /**
   * Test the following schema:
   * CREATE TABLE Singers (
   *   SingerId INT64 NOT NULL,
   *   FirstName STRING(MAX),
   *   LastName STRING(MAX),
   * ) PRIMARY KEY(SingerId);
   *
   * CREATE TABLE Albums (
   *   SingerId INT64 NOT NULL,
   *   AlbumId INT64 NOT NULL,
   *   AlbumName STRING(MAX),
   * ) PRIMARY KEY(SingerId, AlbumId),
   *   INTERLEAVE IN PARENT Singers;
   *
   * CREATE CHANGE STREAM changeStreamName FOR ALL;
   */
  @Test
  public void testChangeStreamTrackAll() {
    mockInformationSchemaChangeStreamsQuery(true);
    mockInformationSchemaTablesQuery();
    mockInformationSchemaColumnsQuery(true);
    mockInformationSchemaKeyColumnUsageQuery(true);

    final String sql =
      "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
        + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
      Statement.newBuilder(sql)
        .bind("changeStreamName").to(changeStreamName)
        .build())).thenReturn(
      ResultSets.forRows(Type.struct(
          Type.StructField.of("TABLE_NAME", Type.string()),
          Type.StructField.of("COLUMN_NAME", Type.string())),
        Collections.emptyList()
      ));

    final List<SpannerColumn> singersPkColumns = Collections.singletonList(
      SpannerColumn.create("SingerId", Type.int64(), 1));
    final List<SpannerColumn> singersNonPkColumns = Arrays.asList(
      SpannerColumn.create("FirstName", Type.string(), 2),
      SpannerColumn.create("LastName", Type.string(), 3));
    final List<SpannerColumn> albumsPkColumns = Arrays.asList(
      SpannerColumn.create("SingerId", Type.int64(), 1),
      SpannerColumn.create("AlbumId", Type.int64(), 2));
    final List<SpannerColumn> albumsNonPkColumns = Collections.singletonList(
      SpannerColumn.create("AlbumName", Type.string(), 3));
    Map<String, SpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
      "Singers", new SpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    expectedSpannerTableByName.put(
      "Albums", new SpannerTable("Albums", albumsPkColumns, albumsNonPkColumns));
    assertEquals(expectedSpannerTableByName,
      SpannerUtils.getSpannerTableByName(mockDatabaseClient, changeStreamName));
  }

  /**
   * Test the following schema (the table schema is the same as the previous test case):
   * CREATE CHANGE STREAM changeStreamName FOR Singers;
   */
  @Test
  public void testChangeStreamTrackOneTable() {
    mockInformationSchemaChangeStreamsQuery(false);
    mockInformationSchemaChangeStreamTablesQuery();
    mockInformationSchemaColumnsQuery(false);
    mockInformationSchemaKeyColumnUsageQuery(false);

    final String sql =
      "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
        + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
      Statement.newBuilder(sql)
        .bind("changeStreamName").to(changeStreamName)
        .build())).thenReturn(
      ResultSets.forRows(Type.struct(
          Type.StructField.of("TABLE_NAME", Type.string()),
          Type.StructField.of("COLUMN_NAME", Type.string())),
        Collections.emptyList()));

    final List<SpannerColumn> singersPkColumns = Collections.singletonList(
      SpannerColumn.create("SingerId", Type.int64(), 1));
    final List<SpannerColumn> singersNonPkColumns = Arrays.asList(
      SpannerColumn.create("FirstName", Type.string(), 2),
      SpannerColumn.create("LastName", Type.string(), 3));
    Map<String, SpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
      "Singers", new SpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertEquals(expectedSpannerTableByName,
      SpannerUtils.getSpannerTableByName(mockDatabaseClient, changeStreamName));
  }

  /**
   * Test the following schema (the table schema is the same as the previous test case):
   * CREATE CHANGE STREAM changeStreamName FOR Singers(SingerId, FirstName);
   */
  @Test
  public void testChangeStreamTrackTwoColumns() {
    mockInformationSchemaChangeStreamsQuery(false);
    mockInformationSchemaChangeStreamTablesQuery();
    mockInformationSchemaColumnsQuery(false);
    mockInformationSchemaKeyColumnUsageQuery(false);

    final String sql =
      "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
        + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
      Statement.newBuilder(sql)
        .bind("changeStreamName").to(changeStreamName)
        .build())).thenReturn(
      ResultSets.forRows(Type.struct(
          Type.StructField.of("TABLE_NAME", Type.string()),
          Type.StructField.of("COLUMN_NAME", Type.string())),
        Arrays.asList(
          Struct.newBuilder()
            .set("TABLE_NAME").to(Value.string("Singers"))
            .set("COLUMN_NAME").to(Value.string("SingerId"))
            .build(),
          Struct.newBuilder()
            .set("TABLE_NAME").to(Value.string("Singers"))
            .set("COLUMN_NAME").to(Value.string("FirstName"))
            .build()
        )));

    final List<SpannerColumn> singersPkColumns = Collections.singletonList(
      SpannerColumn.create("SingerId", Type.int64(), 1));
    final List<SpannerColumn> singersNonPkColumns = Collections.singletonList(
      SpannerColumn.create("FirstName", Type.string(), 2));
    Map<String, SpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
      "Singers", new SpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertEquals(expectedSpannerTableByName,
      SpannerUtils.getSpannerTableByName(mockDatabaseClient, changeStreamName));
  }

  @Test
  public void testSpannerSnapshotRowToBigQueryTableRow() {
    TableRow tableRow = new TableRow();

    final Boolean[] booleanArray = {true, false, true};
    final ByteArray[] bytesArray = {
      ByteArray.copyFrom("123"),
      ByteArray.copyFrom("456"),
      ByteArray.copyFrom("789")
    };
    final Date[] dateArray = {
      Date.fromYearMonthDay(2022, 1, 22),
      Date.fromYearMonthDay(2022, 3, 11)
    };
    final double[] float64Array = {Double.MIN_VALUE, Double.MAX_VALUE, 0, 1, -1, 1.2341};
    final long[] int64Array = {
      Long.MAX_VALUE, Long.MIN_VALUE, 0, 1, -1
    };
    final String[] jsonArray = {"{}", "{\"color\":\"red\",\"value\":\"#f00\"}", "[]"};
    final BigDecimal[] numericArray = {
      BigDecimal.valueOf(1, Integer.MAX_VALUE),
      BigDecimal.valueOf(1, Integer.MIN_VALUE),
      BigDecimal.ZERO,
      BigDecimal.TEN,
      BigDecimal.valueOf(3141592, 6)
    };
    final String[] stringArray = {"abc", "def", "ghi"};
    final Timestamp[] timestampArray = {
      Timestamp.ofTimeSecondsAndNanos(1646617853L, 972000000),
      Timestamp.ofTimeSecondsAndNanos(1646637853L, 572000000),
      Timestamp.ofTimeSecondsAndNanos(1646657853L, 772000000)
    };

    final ResultSet resultSet = ResultSets.forRows(Type.struct(
        Type.StructField.of("BoolCol", Type.bool()),
        Type.StructField.of("BytesCol", Type.bytes()),
        Type.StructField.of("DateCol", Type.date()),
        Type.StructField.of("Float64Col", Type.float64()),
        Type.StructField.of("Int64Col", Type.int64()),
        Type.StructField.of("JsonCol", Type.json()),
        Type.StructField.of("NumericCol", Type.numeric()),
        Type.StructField.of("StringCol", Type.string()),
        Type.StructField.of("TimestampCol", Type.timestamp()),
        Type.StructField.of("BoolArrayCol", Type.array(Type.bool())),
        Type.StructField.of("BytesArrayCol", Type.array(Type.bytes())),
        Type.StructField.of("DateArrayCol", Type.array(Type.date())),
        Type.StructField.of("Float64ArrayCol", Type.array(Type.float64())),
        Type.StructField.of("Int64ArrayCol", Type.array(Type.int64())),
        Type.StructField.of("JsonArrayCol", Type.array(Type.json())),
        Type.StructField.of("NumericArrayCol", Type.array(Type.numeric())),
        Type.StructField.of("StringArrayCol", Type.array(Type.string())),
        Type.StructField.of("TimestampArrayCol", Type.array(Type.timestamp()))
      ),
      Collections.singletonList(
        Struct.newBuilder()
          .set("BoolCol").to(Value.bool(true))
          .set("BytesCol").to(Value.bytes(ByteArray.copyFrom("123")))
          .set("DateCol").to(Date.fromYearMonthDay(2022, 1, 22))
          .set("Float64Col").to(Value.float64(1.2))
          .set("Int64Col").to(Value.int64(20))
          .set("JsonCol").to(Value.json("{\"color\":\"red\",\"value\":\"#f00\"}"))
          .set("NumericCol").to(Value.numeric(BigDecimal.valueOf(123, 2)))
          .set("StringCol").to(Value.string("abc"))
          .set("TimestampCol").to(Timestamp.ofTimeSecondsAndNanos(1646617853L, 972000000))
          .set("BoolArrayCol").to(Value.boolArray(Arrays.asList(booleanArray)))
          .set("BytesArrayCol").to(Value.bytesArray(Arrays.asList(bytesArray)))
          .set("DateArrayCol").to(Value.dateArray(Arrays.asList(dateArray)))
          .set("Float64ArrayCol").to(Value.float64Array(float64Array))
          .set("Int64ArrayCol").to(Value.int64Array(int64Array))
          .set("JsonArrayCol").to(Value.jsonArray(Arrays.asList(jsonArray)))
          .set("NumericArrayCol").to(Value.numericArray(Arrays.asList(numericArray)))
          .set("StringArrayCol").to(Value.stringArray(Arrays.asList(stringArray)))
          .set("TimestampArrayCol").to(Value.timestampArray(Arrays.asList(timestampArray)))
          .build()
      )
    );

    SpannerToBigQueryUtils.spannerSnapshotRowToBigQueryTableRow(
      resultSet, spannerColumnsOfAllTypes, tableRow);
    assertEquals(
      "GenericData{classInfo=[f], " +
        "{BoolCol=true, BytesCol=MTIz, DateCol=2022-01-22, Float64Col=1.2, Int64Col=20, " +
        "JsonCol={\"color\":\"red\",\"value\":\"#f00\"}, " +
        "NumericCol=1.23, StringCol=abc, TimestampCol=2022-03-07T01:50:53.972000000Z, " +
        "BoolArrayCol=[true, false, true], " +
        "BytesArrayCol=[MTIz, NDU2, Nzg5], " +
        "DateArrayCol=[2022-01-22, 2022-03-11], " +
        "Float64ArrayCol=[4.9E-324, 1.7976931348623157E308, 0.0, 1.0, -1.0, 1.2341], " +
        "Int64ArrayCol=[9223372036854775807, -9223372036854775808, 0, 1, -1], " +
        "JsonArrayCol=[{}, {\"color\":\"red\",\"value\":\"#f00\"}, []], " +
        "NumericArrayCol=[1E-2147483647, 1E+2147483648, 0, 10, 3.141592], " +
        "StringArrayCol=[abc, def, ghi], " +
        "TimestampArrayCol=[2022-03-07T01:50:53.972000000Z, 2022-03-07T07:24:13.572000000Z, " +
        "2022-03-07T12:57:33.772000000Z]}}",
      tableRow.toString());
  }

  // TODO: Test the case where some keys are null/empty from Mod.
  @Test
  public void testAppendToSpannerKey() {
    JSONObject keysJsonObject = new JSONObject();
    keysJsonObject.put("BoolCol", true);
    keysJsonObject.put("BytesCol", ByteArray.copyFrom("123").toBase64());
    keysJsonObject.put("DateCol", "2022-01-22");
    keysJsonObject.put("Float64Col", 1.2);
    keysJsonObject.put("Int64Col", 20);
    keysJsonObject.put("NumericCol", new BigDecimal(3.141592));
    keysJsonObject.put("StringCol", "abc");
    keysJsonObject.put("TimestampCol", "2022-03-07T01:50:53.972000000Z");
    Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
    for (final SpannerColumn spannerColumn : spannerColumnsOfAllTypes) {
      String typeName = spannerColumn.getType().getCode().name();
      // Array and JSON are not valid Spanner key type.
      if (typeName.equals("ARRAY") || typeName.equals("JSON")) {
        continue;
      }
      SpannerUtils.appendToSpannerKey(spannerColumn, keysJsonObject, keyBuilder);
    }
    assertEquals("[true,MTIz,2022-01-22,1.2,20," +
        "3.14159200000000016217427400988526642322540283203125,abc,2022-03-07T01:50:53.972000000Z]",
      keyBuilder.build().toString());
  }

  @Test
  public void testSpannerColumnsToBigQueryIOFields() {
    String bigQueryIOFieldsStr = SpannerToBigQueryUtils.spannerColumnsToBigQueryIOFields(
      spannerColumnsOfAllTypes).toString();
    // Remove redundant information.
    bigQueryIOFieldsStr = bigQueryIOFieldsStr.replace(
      "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name," +
        " policyTags, precision, scale, type], ", "");
    bigQueryIOFieldsStr = bigQueryIOFieldsStr.replace("GenericData", "");
    assertEquals(
      "[{{mode=NULLABLE, name=BoolCol, type=BOOL}}, " +
        "{{mode=NULLABLE, name=BytesCol, type=BYTES}}, " +
        "{{mode=NULLABLE, name=DateCol, type=DATE}}, " +
        "{{mode=NULLABLE, name=Float64Col, type=FLOAT64}}, " +
        "{{mode=NULLABLE, name=Int64Col, type=INT64}}, " +
        "{{mode=NULLABLE, name=JsonCol, type=STRING}}, " +
        "{{mode=NULLABLE, name=NumericCol, type=NUMERIC}}, " +
        "{{mode=NULLABLE, name=StringCol, type=STRING}}, " +
        "{{mode=NULLABLE, name=TimestampCol, type=TIMESTAMP}}, " +
        "{{mode=REPEATED, name=BoolArrayCol, type=BOOL}}, " +
        "{{mode=REPEATED, name=BytesArrayCol, type=BYTES}}, " +
        "{{mode=REPEATED, name=DateArrayCol, type=DATE}}, " +
        "{{mode=REPEATED, name=Float64ArrayCol, type=FLOAT64}}, " +
        "{{mode=REPEATED, name=Int64ArrayCol, type=INT64}}, " +
        "{{mode=REPEATED, name=JsonArrayCol, type=STRING}}, " +
        "{{mode=REPEATED, name=NumericArrayCol, type=NUMERIC}}, " +
        "{{mode=REPEATED, name=StringArrayCol, type=STRING}}, " +
        "{{mode=REPEATED, name=TimestampArrayCol, type=TIMESTAMP}}]",
      bigQueryIOFieldsStr);
  }
}
