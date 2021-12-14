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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileDeduplicator {

  private static final Pattern FILE_PATTERN = Pattern.compile("(.*)-\\d+(\\.\\d+)?(\\.\\d+)?(\\.jar)$");

  public List<String> deduplicate(List<String> filePaths) {
    final Map<String, String> fileNameToPath = new HashMap<>();
    for (String filePath : filePaths) {
      final File file = new File(filePath);
      final String fileName = removeVersion(file.getName());
      if (!fileNameToPath.containsKey(fileName)) {
        fileNameToPath.put(fileName, filePath);
      }
    }

    return new ArrayList<>(fileNameToPath.values());
  }

  private String removeVersion(String fileName) {
    if (fileName == null) return fileName;
    if (fileName.trim().equals("")) return fileName;

    final Matcher matcher = FILE_PATTERN.matcher(fileName);
    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return fileName;
    }
  }

}
