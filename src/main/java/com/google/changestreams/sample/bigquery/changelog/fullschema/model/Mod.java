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
package com.google.changestreams.sample.bigquery.changelog.fullschema.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.changestreams.sample.bigquery.changelog.fullschema.BigQueryDynamicDestinations;
import com.google.cloud.Timestamp;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Class {@link Mod} represents contains the keys and metadata of a Spanner row. Note it's different
 * from the {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod}.
 */
@DefaultCoder(AvroCoder.class)
public class Mod implements Serializable {

  private static final long serialVersionUID = 8703257194338184299L;

  private String keysJson;
  private long commitTimestampSeconds;
  private int commitTimestampNanos;
  private String serverTransactionId;
  private boolean isLastRecordInTransactionInPartition;
  private String recordSequence;
  private String tableName;
  private ModType modType;
  private long numberOfRecordsInTransaction;
  private long numberOfPartitionsInTransaction;

  /**
   * Default constructor for serialization only.
   */
  private Mod() {}

  /**
   * @param keysJson                             JSON object as String, where the keys are the primary key column names and the
   *                                             values are the primary key column values
   * @param commitTimestamp                      the timestamp at which the modifications within were committed in Cloud
   *                                             Spanner
   * @param serverTransactionId                  the unique transaction id in which the modifications occurred
   * @param isLastRecordInTransactionInPartition indicates whether this record is the last emitted
   *                                             for the given transaction in the given partition
   * @param recordSequence                       indicates the order in which this record was put into the change stream
   *                                             in the scope of a partition, commit timestamp and transaction tuple
   * @param tableName                            the name of the table in which the modifications occurred
   * @param modType                              the operation that caused the modification to occur
   * @param numberOfRecordsInTransaction         the total number of records for the given transaction
   * @param numberOfPartitionsInTransaction      the total number of partitions within the given
   *                                             transaction
   * @return a Mod corresponding to a DataChangeRecord.Mod. The constructed Mod is used as the
   * processing unit of the pipeline, it contains all the information from DataChangeRecord and
   * DataChangeRecord.Mod, except columns JSON, since we will read the columns from Spanner instead.
   */
  public Mod(
    String keysJson,
    Timestamp commitTimestamp,
    String serverTransactionId,
    boolean isLastRecordInTransactionInPartition,
    String recordSequence,
    String tableName,
    ModType modType,
    long numberOfRecordsInTransaction,
    long numberOfPartitionsInTransaction) {
    this.keysJson = keysJson;
    this.commitTimestampSeconds = commitTimestamp.getSeconds();
    this.commitTimestampNanos = commitTimestamp.getNanos();
    this.serverTransactionId = serverTransactionId;
    this.isLastRecordInTransactionInPartition = isLastRecordInTransactionInPartition;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.modType = modType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
  }

  public static Mod fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, Mod.class);
  }

  /**
   * The primary keys of this specific modification. This is always present and can not be null. The
   * keys are returned as a JSON object (stringified), where the keys are the column names and the
   * values are the column values.
   *
   * @return JSON object as String representing the primary key state for the row modified
   */
  public String getKeysJson() {
    return keysJson;
  }

  /**
   * The seconds part of the timestamp at which the modifications within were committed in Cloud
   * Spanner.
   */
  public long getCommitTimestampSeconds() {
    return commitTimestampSeconds;
  }

  /**
   * The nanoseconds part of the timestamp at which the modifications within were committed in
   * Cloud Spanner.
   */
  public int getCommitTimestampNanos() {
    return commitTimestampNanos;
  }

  /**
   * The unique transaction id in which the modifications occurred.
   */
  public String getServerTransactionId() {
    return serverTransactionId;
  }

  /**
   * Indicates whether this record is the last emitted for the given transaction in the given
   * partition.
   */
  public boolean getIsLastRecordInTransactionInPartition() {
    return isLastRecordInTransactionInPartition;
  }

  /**
   * Indicates the order in which this record was put into the change stream in the scope of a
   * partition, commit timestamp and transaction tuple.
   */
  public String getRecordSequence() {
    return recordSequence;
  }

  /**
   * The name of the table in which the modifications within this record occurred.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * The type of operation that caused the modifications within this record.
   */
  public ModType getModType() {
    return modType;
  }

  /**
   * The total number of data change records for the given transaction.
   */
  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  /**
   * The total number of partitions for the given transaction.
   */
  public long getNumberOfPartitionsInTransaction() {
    return numberOfPartitionsInTransaction;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Mod)) {
      return false;
    }
    Mod that = (Mod) o;
    return keysJson == that.keysJson
      && isLastRecordInTransactionInPartition == that.isLastRecordInTransactionInPartition
      && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
      && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
      && commitTimestampSeconds == that.commitTimestampSeconds
      && commitTimestampNanos == that.commitTimestampNanos
      && Objects.equals(serverTransactionId, that.serverTransactionId)
      && Objects.equals(recordSequence, that.recordSequence)
      && Objects.equals(tableName, that.tableName)
      && modType == that.modType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
      keysJson,
      commitTimestampSeconds,
      commitTimestampNanos,
      serverTransactionId,
      isLastRecordInTransactionInPartition,
      recordSequence,
      tableName,
      modType,
      numberOfRecordsInTransaction,
      numberOfPartitionsInTransaction);
  }

  @Override
  public String toString() {
    return "Mod{"
      + "keysJson='"
      + keysJson
      + '\''
      + ", commitTimestampSeconds="
      + commitTimestampSeconds
      + ", commitTimestampNanos="
      + commitTimestampNanos
      + ", serverTransactionId='"
      + serverTransactionId
      + '\''
      + ", isLastRecordInTransactionInPartition="
      + isLastRecordInTransactionInPartition
      + ", recordSequence='"
      + recordSequence
      + '\''
      + ", tableName='"
      + tableName
      + '\''
      + ", modType="
      + modType
      + ", numberOfRecordsInTransaction="
      + numberOfRecordsInTransaction
      + ", numberOfPartitionsInTransaction="
      + numberOfPartitionsInTransaction
      + '}';
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}