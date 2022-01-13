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

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.*;

@DefaultCoder(AvroCoder.class)
public class SpannerTable implements Serializable {

  private String tableName;
  private List<SpannerColumn> pkColumns;
  private List<SpannerColumn> nonPkColumns;
  private List<SpannerColumn> allColumns;

  /**
   * Default constructor for serialization only.
   */
  public SpannerTable() {}

  private static class SortByOrdinalPosition implements Comparator<SpannerColumn> {
    public int compare(SpannerColumn o1, SpannerColumn o2) {
      return Integer.compare(o1.getOrdinalPosition(), o2.getOrdinalPosition());
    }
  }

  public SpannerTable(String tableName, List<SpannerColumn> pkColumns,
                      List<SpannerColumn> nonPkColumns) {
    Collections.sort(pkColumns, new SortByOrdinalPosition());
    Collections.sort(nonPkColumns, new SortByOrdinalPosition());
    this.tableName = tableName;
    this.pkColumns = pkColumns;
    this.nonPkColumns = nonPkColumns;

    this.allColumns = new LinkedList<>();
    allColumns.addAll(pkColumns);
    allColumns.addAll(nonPkColumns);
  }

  public String getTableName() {
    return tableName;
  }

  public List<SpannerColumn> getPkColumns() {
    return pkColumns;
  }

  public List<SpannerColumn> getNonPkColumns() {
    return nonPkColumns;
  }

  public List<SpannerColumn> getAllColumns() {
    return allColumns;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpannerTable)) {
      return false;
    }
    SpannerTable that = (SpannerTable) o;
    return Objects.equals(tableName, that.tableName)
      && Objects.equals(pkColumns, that.pkColumns)
      && Objects.equals(nonPkColumns, that.nonPkColumns)
      && Objects.equals(pkColumns, that.pkColumns);
  }

  @Override
  public String toString() {
    return "SpannerTable{" +
      "tableName='" + tableName + '\'' +
      ", pkColumns=" + pkColumns +
      ", nonPkColumns=" + nonPkColumns +
      ", allColumns=" + allColumns +
      '}';
  }
}
