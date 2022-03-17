package com.google.changestreams.sample.bigquery.changelog.fullschema.schemautils;

import java.util.HashSet;
import java.util.Set;

public class BigQueryUtils {

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
}
