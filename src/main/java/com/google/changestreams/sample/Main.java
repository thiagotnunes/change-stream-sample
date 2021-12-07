/*
 * Copyright 2021 Google LLC
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

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import com.google.cloud.Timestamp;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class Main {

  public static void main(String[] args) {
    final SampleOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .as(SampleOptions.class);
    options.setFilesToStage(deduplicateFilesToStage(options));
    final Pipeline pipeline = Pipeline.create(options);

    final String projectId = options.getProject();
    final String instanceId = options.getInstance();
    final String databaseId = options.getDatabase();
    final String metadataInstanceId = options.getMetadataInstance();
    final String metadataDatabaseId = options.getMetadataDatabase();
    final String changeStreamName = options.getChangeStreamName();
    final String gcsBucket = options.getGcsBucket();
    final Timestamp now = Timestamp.now();
    final Timestamp after10Minutes = Timestamp.ofTimeSecondsAndNanos(
        now.getSeconds() + (10 * 60),
        now.getNanos()
    );
    final String gcsFilePrefix = "gs://" + gcsBucket + "/change-streams-sample/" + now + "/output";

    pipeline
        // Reads from the change stream
        .apply(SpannerIO
            .readChangeStream()
            .withSpannerConfig(SpannerConfig
                .create()
                .withProjectId(projectId)
                .withInstanceId(instanceId)
                .withDatabaseId(databaseId)
            )
            .withMetadataInstance(metadataInstanceId)
            .withMetadataDatabase(metadataDatabaseId)
            .withChangeStreamName(changeStreamName)
            .withInclusiveStartAt(now)
            .withInclusiveEndAt(after10Minutes)
        )

        // Maps records to strings (commit timestamp only)
        .apply(MapElements
            .into(TypeDescriptors.strings())
            .via(record -> record.toString())
        )

        // Group records into 1 minute windows
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))

        // Writes each window of records into GCS
        .apply(TextIO
            .write()
            .to(gcsFilePrefix)
            .withSuffix(".txt")
            .withWindowedWrites()
            .withNumShards(1)
        );

    pipeline.run().waitUntilFinish();
  }

  /**
   * This is to avoid a bug in Dataflow, where if there are duplicate jar files to stage, the job
   * gets stuck. Before submitting the job we deduplicate the jar files here.
   */
  public static List<String> deduplicateFilesToStage(DataflowPipelineOptions options) {
    final Map<String, String> fileNameToPath = new HashMap<>();
    final List<String> filePaths =
        detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), options);

    for (String filePath : filePaths) {
      final File file = new File(filePath);
      final String fileName = file.getName();
      if (!fileNameToPath.containsKey(fileName)) {
        fileNameToPath.put(fileName, filePath);
      }
    }

    return new ArrayList<>(fileNameToPath.values());
  }
}
