/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.changestreams.sample;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface ChangeStreamPipelineOptions extends DataflowPipelineOptions {
  String getInstance();

  void setInstance(String instance);

  String getDatabase();

  void setDatabase(String database);

  String getMetadataInstance();

  void setMetadataInstance(String metadataInstance);

  String getMetadataDatabase();

  void setMetadataDatabase(String metadataDatabase);

  String getChangeStreamName();

  void setChangeStreamName(String changeStreamName);
}
