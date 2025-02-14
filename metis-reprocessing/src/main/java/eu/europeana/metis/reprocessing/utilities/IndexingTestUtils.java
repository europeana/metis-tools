package eu.europeana.metis.reprocessing.utilities;

import java.io.IOException;

public class IndexingTestUtils {
  /**
   * Gets resource file content.
   *
   * @param fileName the file name
   * @return the resource file content
   */
  public static String getResourceFileContent(String fileName) {
    try {
      return new String(IndexingTestUtils.class.getClassLoader().getResourceAsStream(fileName).readAllBytes());
    } catch (IOException ioException) {
      return "";
    }
  }
}
