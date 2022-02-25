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

package com.google.changestreams.sample.pubsub;

import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class Main {

  public static void main(String[] args) {
    final PubsubPipelineOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .as(PubsubPipelineOptions.class);
    final Pipeline pipeline = Pipeline.create(options);

    final String projectId = options.getProject();
    final String instanceId = options.getInstance();
    final String databaseId = options.getDatabase();
    final String metadataInstanceId = options.getMetadataInstance();
    final String metadataDatabaseId = options.getMetadataDatabase();
    final String changeStreamName = options.getChangeStreamName();
    final String pubsubTopic = options.getPubsubTopic();
    final Timestamp now = Timestamp.now();
    final Timestamp after10Minutes = Timestamp.ofTimeSecondsAndNanos(
        now.getSeconds() + (10 * 60),
        now.getNanos()
    );
    final PublishToPubsubDoFn publishToPubsubDoFn = new PublishToPubsubDoFn(projectId, pubsubTopic);

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

        // Publishes to Pubsub
        .apply(ParDo.of(publishToPubsubDoFn));

    pipeline.run().waitUntilFinish();
  }

  public static class PublishToPubsubDoFn extends DoFn<DataChangeRecord, String> {
    private final String projectId;
    private final String topic;
    private transient Publisher publisher;

    public PublishToPubsubDoFn(String projectId, String topic) {
      this.projectId = projectId;
      this.topic = topic;
    }

    @Setup
    public void setup() {
      try {
        final TopicName topicName = TopicName.of(projectId, topic);
        publisher = Publisher
            .newBuilder(topicName)
            .build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Teardown
    public void tearDown() {
      try {
        if (publisher != null) {
          publisher.shutdown();
          publisher.awaitTermination(5, TimeUnit.MINUTES);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @ProcessElement
    public void processElement(@Element DataChangeRecord record) {
      try {
        final String messageId = record.getPartitionToken()
            + ";"
            + record.getServerTransactionId()
            + ";"
            + record.getRecordSequence();
        final ByteString data = ByteString.copyFromUtf8(record.toString());
        publisher.publish(PubsubMessage
            .newBuilder()
            .setData(data)
            .setMessageId(messageId)
            .build()
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
