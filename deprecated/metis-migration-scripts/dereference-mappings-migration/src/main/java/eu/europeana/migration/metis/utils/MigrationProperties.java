package eu.europeana.migration.metis.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import eu.europeana.migration.metis.MigrationFromUimToMetis;

/**
 * This class is responsible for reading the configuration file and giving out the properties in
 * that file.
 * 
 * @author jochen
 *
 */
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

  /**
   * Constructor.
   * 
   * @param sourceMongoUrl The url of the source database.
   * @param sourceMongoDbName The database name in the source database.
   * @param targetMongoUrl The url of the target database.
   * @param targetMongoDbName The database name in the target database.
   */
  public MigrationProperties(String sourceMongoUrl, String sourceMongoDbName, String targetMongoUrl,
      String targetMongoDbName) {
    this.sourceMongoUrl = sourceMongoUrl;
    this.sourceMongoDbName = sourceMongoDbName;
    this.targetMongoUrl = targetMongoUrl;
    this.targetMongoDbName = targetMongoDbName;
  }

  /**
   * Read the property file. In case the file could not be read or one of the required properties is
   * missing an exception is thrown.
   * 
   * @param propertiesFilePath The path of the properties file. Can be null, in which case the
   *        method will look for a file named {@value #DEFAULT_PROPERTIES_NAME} in the class path.
   * @return An instance of this class containing the properties. Is not null.
   * @throws IOException
   */
  public static MigrationProperties readFromFile(String propertiesFilePath) throws IOException {

    final String fileNameForMessages = propertiesFilePath != null ? propertiesFilePath
        : (DEFAULT_PROPERTIES_NAME + " (default properties file)");
    LogUtils.logInfoMessage("Reading property file: " + fileNameForMessages);

    final Properties properties = new Properties();
    final InputStream input;
    if (propertiesFilePath != null) {
      input = new FileInputStream(propertiesFilePath);
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

  /**
   * 
   * @return The url of the source database.
   */
  public String getSourceMongoUrl() {
    return sourceMongoUrl;
  }

  /**
   * 
   * @return The name of the source database.
   */
  public String getSourceMongoDbName() {
    return sourceMongoDbName;
  }

  /**
   * 
   * @return The url of the target database.
   */
  public String getTargetMongoUrl() {
    return targetMongoUrl;
  }

  /**
   * 
   * @return The name of the target database.
   */
  public String getTargetMongoDbName() {
    return targetMongoDbName;
  }
}
