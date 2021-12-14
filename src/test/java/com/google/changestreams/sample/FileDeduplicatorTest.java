package com.google.changestreams.sample;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class FileDeduplicatorTest {

  @Test
  public void testDeduplicateFiles() {
    final List<String> filePaths = Arrays.asList(
        "/Users/user/.m2/repository/org/apache/beam/beam-sdks-java-extensions-google-cloud-platform-core/2.32.0/beam-sdks-java-extensions-google-cloud-platform-core-2.32.0.jar",
        "/usr/local/Cellar/maven/3.8.4/libexec/lib/beam-sdks-java-extensions-google-cloud-platform-core-2.32.0.jar",
        "/Users/user/.m2/repository/commons-io/commons-io/2.5/commons-io-2.5.jar",
        "/usr/local/Cellar/maven/3.8.4/libexec/lib/commons-io-2.6.jar",
        "/Users/user/.m2/repository/org/apache/avro/avro/1.8.2/avro-1.8.2.jar",
        "/usr/local/Cellar/maven/3.8.4/libexec/lib/avro/avro/1.8.2/avro-1.8.2.jar",
        "/Users/user/.m2/repository/org/apache/test/test/1/test-1.jar",
        "/usr/local/Cellar/maven/3.8.4/libexec/lib/test/test/1/test-2.jar"
    );
    final List<String> expected = Arrays.asList(
        "/Users/user/.m2/repository/commons-io/commons-io/2.5/commons-io-2.5.jar",
        "/Users/user/.m2/repository/org/apache/beam/beam-sdks-java-extensions-google-cloud-platform-core/2.32.0/beam-sdks-java-extensions-google-cloud-platform-core-2.32.0.jar",
        "/Users/user/.m2/repository/org/apache/avro/avro/1.8.2/avro-1.8.2.jar",
        "/Users/user/.m2/repository/org/apache/test/test/1/test-1.jar"
    );

    final List<String> actual = new FileDeduplicator().deduplicate(filePaths);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
  }
}
