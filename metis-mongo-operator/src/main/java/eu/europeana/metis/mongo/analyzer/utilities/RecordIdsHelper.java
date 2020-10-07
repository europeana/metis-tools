package eu.europeana.metis.mongo.analyzer.utilities;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang.StringUtils;

public class RecordIdsHelper {

  private RecordIdsHelper() {
  }

  public static List<String> getRecordIds(String recordAboutToCheck,
      Path pathWithCorruptedRecords) {
    final List<String> recordAbouts;
    if (StringUtils.isBlank(recordAboutToCheck)) {
      //Read all corrupted record abouts
      try {
        recordAbouts = Files.readAllLines(pathWithCorruptedRecords);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to read file %s", pathWithCorruptedRecords), e);
      }
    } else {
      recordAbouts = List.of(recordAboutToCheck);
    }
    return recordAbouts;
  }
}
