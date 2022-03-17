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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface Options extends DataflowPipelineOptions {

  @Description("The Spanner instance that contains the change stream")
  @Validation.Required
  String getSpannerInstance();

  void setSpannerInstance(String value);

  @Description("The Spanner database that contains the change stream")
  @Validation.Required
  String getSpannerDatabase();

  void setSpannerDatabase(String value);

  @Description("The Spanner metadata instance that's used by the change stream connector")
  @Validation.Required
  String getSpannerMetadataInstance();

  void setSpannerMetadataInstance(String value);

  @Description("The Spanner metadata database that's used by the change stream connector")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String value);

  @Description("The name of the change stream")
  @Validation.Required
  String getChangeStreamName();

  void setChangeStreamName(String value);

  @Description(
    "The start timestamp used to fetch from change stream, default to now.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String value);

  @Description("The BigQuery dataset.")
  @Validation.Required
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @Description("The end timestamp used to fetch from change stream.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String value);

  @Description("Spanner host endpoint (only used for testing).")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @Description("The BigQuery Project ID. Default is the project for the Dataflow job.")
  @Default.String("")
  String getBigQueryProjectId();

  void setBigQueryProjectId(String value);

  @Description("The changelog BigQuery table Template")
  @Default.String("{_metadata_spanner_table_name}_changelog")
  String getBigQueryChangelogTableNameTemplate();

  void setBigQueryChangelogTableNameTemplate(String value);

  // TODO: Implement this functionality.
  @Description("If specified, the BigQuery changelog table will be created as partition table at specified granularity.")
  @Default.String("")
  String getBigQueryChangelogTablePartitionGranularity();

  void setBigQueryChangelogTablePartitionGranularity(String value);

  // TODO: Implement this functionality.
  @Description("If specified, the BigQuery changelog table set to the expiration time.")
  @Default.Long(0)
  String getBigQueryChangelogTablePartitionExpirationMs();

  void setBigQueryChangelogTablePartitionExpirationMs(String value);

  @Description("The number of minutes between DLQ Retries")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  @Description("The Dead Letter Queue GCS Prefix to use for errored data")
  @Default.String("")
  String getDeadLetterQueueDirectory();

  void setDeadLetterQueueDirectory(String value);

  // TODO: Implement ignoreFields functionality.
}
