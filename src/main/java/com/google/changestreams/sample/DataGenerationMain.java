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

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "data-generation", mixinStandardHelpOptions = true,
    version = "data-generation 0.0.1",
    description = "Creates a Cloud Spanner database, tables and change stream if they do not exist. Generates load in the created resources.")
public class DataGenerationMain implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerationMain.class);
  private static final Duration TIMEOUT_DURATION = Duration.ofMinutes(5);

  @Option(
      names = {"-p", "--project"},
      required = true,
      description = "The Cloud Spanner project to be used")
  private String projectId;

  @Option(
      names = {"-i", "--instance"},
      required = true,
      description = "The Cloud Spanner instance to be used")
  private String instanceId;

  @Option(
      names = {"-l", "--ldap"},
      required = true,
      description = "Your LDAP to be used to for the Cloud Spanner database id and change stream name. The Database will be of the form <ldap>-hackathon. The change stream name will be of the form <ldap>ChangeStream.")
  private String ldap;

  @Override
  public Integer call() throws Exception {
    final String databaseId = String.format("%s-hackathon", ldap);
    final DatabaseId id = DatabaseId.of(projectId, instanceId, databaseId);
    final String changeStreamName = String.format("%sChangeStream", ldap);
    final SpannerOptions options = SpannerOptions.newBuilder().setProjectId(projectId).build();
    final Spanner spanner = options.getService();
    final DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    createDatabaseIfNotExists(databaseAdminClient, id);
    createTableIfNotExists(databaseAdminClient, id);
    createChangeStreamIfNotExists(databaseAdminClient, id, changeStreamName);
    generateLoad(spanner.getDatabaseClient(id));

    return 0;
  }

  private void generateLoad(DatabaseClient databaseClient)
      throws InterruptedException {
    final Random random = new Random();
    LOGGER.info("Starting data generation");
    while (true) {
      for (int i = 0; i < 10; i++) {
        final int singerId = random.nextInt();
        databaseClient.write(Collections.singletonList(
            Mutation
                .newInsertOrUpdateBuilder("Singers")
                .set("SingerId")
                .to(singerId)
                .set("FirstName")
                .to(String.format("FirstName-%d", singerId))
                .set("LastName")
                .to(String.format("LastName-%d", singerId))
                .build()
        ));
      }
      LOGGER.info("Inserted / updated 10 random records, resuming in 1 second");
      Thread.sleep(1_000L);
    }
  }

  private void createChangeStreamIfNotExists(
      DatabaseAdminClient databaseAdminClient,
      DatabaseId id,
      String changeStreamName)
      throws ExecutionException, InterruptedException, TimeoutException {
    final List<String> ddls = databaseAdminClient
        .getDatabaseDdl(id.getInstanceId().getInstance(), id.getDatabase());
    for (String ddl : ddls) {
      if (ddl.contains(changeStreamName)) {
        LOGGER.info("Change stream " + changeStreamName + " already exists, using it...");
        return;
      }
    }
    LOGGER.info("Change stream " + changeStreamName + " does not exist in " + id + ", creating it");
    databaseAdminClient
        .updateDatabaseDdl(
            id.getInstanceId().getInstance(),
            id.getDatabase(),
            Collections.singletonList(
                "CREATE CHANGE STREAM "
                    + changeStreamName
                    + " FOR Singers"),
            null
        ).get(TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
    LOGGER.info("Change stream " + changeStreamName + " created successfully");
  }

  private void createTableIfNotExists(
      DatabaseAdminClient databaseAdminClient,
      DatabaseId id)
      throws ExecutionException, InterruptedException, TimeoutException {
    final List<String> ddls = databaseAdminClient
        .getDatabaseDdl(id.getInstanceId().getInstance(), id.getDatabase());
    for (String ddl : ddls) {
      if (ddl.contains("Singers")) {
        LOGGER.info("Table Singers already exists in " + id + ", using it...");
        return;
      }
    }
    LOGGER.info("Table Singers does not exist in " + id + ", creating it...");
    databaseAdminClient
        .updateDatabaseDdl(
            id.getInstanceId().getInstance(),
            id.getDatabase(),
            Collections.singletonList(
                "CREATE TABLE Singers ("
                    + "  SingerId   INT64 NOT NULL,"
                    + "  FirstName  STRING(1024),"
                    + "  LastName   STRING(1024),"
                    + "  SingerInfo BYTES(MAX)"
                    + ") PRIMARY KEY (SingerId)"
            ),
            null
        )
        .get(TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
    LOGGER.info("Table Singers created successfully");
  }

  private void createDatabaseIfNotExists(DatabaseAdminClient databaseAdminClient, DatabaseId id)
      throws ExecutionException, InterruptedException, TimeoutException {
    try {
      databaseAdminClient
          .getDatabase(id.getInstanceId().getInstance(), id.getDatabase());
      LOGGER.info("Database " + id + " already exists, using it...");
    } catch (DatabaseNotFoundException e) {
      LOGGER.info("Database " + id + " does not exist, creating it...");
      databaseAdminClient
          .createDatabase(id.getInstanceId().getInstance(), id.getDatabase(),
              Collections.emptyList())
          .get(TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
      LOGGER.info("Database " + id + " created successfully");
    }
  }

  public static void main(String[] args) {
    final int exitCode = new CommandLine(new DataGenerationMain()).execute(args);
    System.exit(exitCode);
  }
}
