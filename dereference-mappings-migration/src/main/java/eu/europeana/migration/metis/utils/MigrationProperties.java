package eu.europeana.migration.metis.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import eu.europeana.migration.metis.MigrationFromUimToMetis;

public class MigrationProperties {

  private static final String DEFAULT_PROPERTIES_NAME = "migration.properties";

  private static final String SOURCE_MONGO_URL_PROPERTY = "source.mongo.url";
  private static final String SOURCE_MONGO_DB_NAME_PROPERTY = "source.mongo.databasename";
  private static final String TARGET_MONGO_URL_PROPERTY = "target.mongo.url";
  private static final String TARGET_MONGO_DB_NAME_PROPERTY = "target.mongo.databasename";

  private final String sourceMongoUrl;
  private final String sourceMongoDbName;
  private final String targetMongoUrl;
  private final String targetMongoDbName;

  public MigrationProperties(String sourceMongoUrl, String sourceMongoDbName, String targetMongoUrl,
      String targetMongoDbName) {
    this.sourceMongoUrl = sourceMongoUrl;
    this.sourceMongoDbName = sourceMongoDbName;
    this.targetMongoUrl = targetMongoUrl;
    this.targetMongoDbName = targetMongoDbName;
  }

  public static MigrationProperties readFromFile(String propertiesFileName) throws IOException {

    final String fileNameForMessages = propertiesFileName != null ? propertiesFileName
        : (DEFAULT_PROPERTIES_NAME + " (default properties file)");
    LogUtils.logInfoMessage("Reading property file: " + propertiesFileName);

    final Properties properties = new Properties();
    final InputStream input;
    if (propertiesFileName != null) {
      input = new FileInputStream(propertiesFileName);
    } else {
      input = MigrationFromUimToMetis.class.getClassLoader()
          .getResourceAsStream(DEFAULT_PROPERTIES_NAME);
    }
    properties.load(input);

    final String sourceMongoUrl =
        verifyFieldPresence(SOURCE_MONGO_URL_PROPERTY, properties, fileNameForMessages);
    final String sourceMongoDbName =
        verifyFieldPresence(SOURCE_MONGO_DB_NAME_PROPERTY, properties, fileNameForMessages);
    final String targetMongoUrl =
        verifyFieldPresence(TARGET_MONGO_URL_PROPERTY, properties, fileNameForMessages);
    final String targetMongoDbName =
        verifyFieldPresence(TARGET_MONGO_DB_NAME_PROPERTY, properties, fileNameForMessages);

    return new MigrationProperties(sourceMongoUrl, sourceMongoDbName, targetMongoUrl,
        targetMongoDbName);
  }

  private static String verifyFieldPresence(String fieldName, Properties properties,
      String fileName) {
    if (!properties.containsKey(fieldName) || properties.getProperty(fieldName).trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Properties file '" + fileName + "' does not contain '" + fieldName + "' property.");
    }
    final String result = properties.getProperty(fieldName).trim();
    LogUtils.logInfoMessage("--  Property " + fieldName + " set to: " + result);
    return result;
  }

  public String getSourceMongoUrl() {
    return sourceMongoUrl;
  }

  public String getSourceMongoDbName() {
    return sourceMongoDbName;
  }

  public String getTargetMongoUrl() {
    return targetMongoUrl;
  }

  public String getTargetMongoDbName() {
    return targetMongoDbName;
  }
}
