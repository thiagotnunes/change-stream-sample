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

package com.google.changestreams.sample.gcs;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class Main {

  public static void main(String[] args) {
    final GcsPipelineOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .as(GcsPipelineOptions.class);
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

        // Maps records to strings
        .apply(MapElements
            .into(TypeDescriptors.strings())
            .via(DataChangeRecord::toString)
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
}
