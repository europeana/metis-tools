package eu.europeana.metis.tools;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.opencsv.CSVReader;
import eu.europeana.metis.core.common.Country;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.plugins.OaipmhHarvestPluginMetadata;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-13
 */
public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Marker FAILED_CSV_LINES_MARKER = MarkerFactory.getMarker("FAILED_CSV_LINES");
  private static final Marker EXECUTION_LOGS_MARKER = MarkerFactory.getMarker("EXECUTION_LOGS");
  private static final Marker SUCCESSFULL_DATASET_IDS = MarkerFactory
      .getMarker("SUCCESSFULL_DATASET_IDS");

  private static final String ORGANIZATION_ID;
  private static final String ORGANIZATION_NAME;
  private static final String USER_ID;
  private static final String DATASETS_CSV_PATH;
  private static final Mode MODE;
  private static final String DATASET_IDS_PATH;

  private static final String TRUSTSTORE_PATH;
  private static final String TRUSTSTORE_PASSWORD;
  private static final String SUGARCRM_DATE_FORMAT = "dd-MM-yyyy HH:mm";
  private static final String[] MONGO_HOSTS;
  private static final int[] MONGO_PORTS;
  private static final String MONGO_AUTHENTICATION_DB;
  private static final String MONGO_USERNAME;
  private static final String MONGO_PASSWORD;
  private static final boolean MONGO_ENABLESSL;
  private static final String MONGO_DB;

  private static MongoClient mongoClient;
  private static DatasetDao datasetDao;
  private static int readCounter = 0;
  private static int storedCounter = 0;
  private static int failedCounter = 0;
  private static int deletedCounter = 0;

  static {
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(Thread.currentThread().getContextClassLoader()
          .getResource("migration.properties").getFile()));
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
    ORGANIZATION_ID = properties.getProperty("organization.id");
    ORGANIZATION_NAME = properties.getProperty("organization.name");
    USER_ID = properties.getProperty("user.id");
    DATASETS_CSV_PATH = properties.getProperty("datasets.csv.path");
    MODE = Mode.getModeFromEnumName(properties.getProperty("mode"));
    DATASET_IDS_PATH = properties.getProperty("dataset.ids.path");
    TRUSTSTORE_PATH = properties.getProperty("truststore.path");
    TRUSTSTORE_PASSWORD = properties.getProperty("truststore.password");
    MONGO_HOSTS = properties.getProperty("mongo.hosts").split(",");
    MONGO_PORTS = Arrays.stream(properties.getProperty("mongo.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    MONGO_AUTHENTICATION_DB = properties.getProperty("mongo.authentication.db");
    MONGO_USERNAME = properties.getProperty("mongo.username");
    MONGO_PASSWORD = properties.getProperty("mongo.password");
    MONGO_ENABLESSL = Boolean.parseBoolean(properties.getProperty("mongo.enableSSL"));
    MONGO_DB = properties.getProperty("mongo.db");
  }

  public static void main(String[] args) throws TrustStoreConfigurationException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Starting migration script");
    LOGGER.info(EXECUTION_LOGS_MARKER, "Append default trustore with custom trustore");
    if (StringUtils.isNotEmpty(TRUSTSTORE_PATH) && StringUtils
        .isNotEmpty(TRUSTSTORE_PASSWORD)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Inialize mongo connection");
    initializeMongoClient();
    datasetDao = new DatasetDao(new MorphiaDatastoreProvider(mongoClient, MONGO_DB));

    switch (MODE) {
      case CREATE:
        LOGGER.info(EXECUTION_LOGS_MARKER, "Mode CREATE");
        createMode();
        break;
      case DELETE:
        LOGGER.info(EXECUTION_LOGS_MARKER, "Mode DELETE");
        deleteMode();
        break;
      default:
        LOGGER.info(EXECUTION_LOGS_MARKER, "Mode not supported.");
        break;
    }
    mongoClient.close();
  }

  private static void createMode() {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Start reading the csv file");
    try (CSVReader reader = new CSVReader(new FileReader(DATASETS_CSV_PATH))) {
      String[] line;
      reader.readNext();//Bypass titles line
      while ((line = reader.readNext()) != null) {
        readCounter++;
        convertLineToDataset(line, datasetDao);
      }
    } catch (IOException e) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "Reading csv file failed ", e);
    }

    LOGGER.error(EXECUTION_LOGS_MARKER, "Total lines read: {} ", readCounter);
    LOGGER.error(EXECUTION_LOGS_MARKER, "Total datasets stored: {} ", storedCounter);
    LOGGER.error(EXECUTION_LOGS_MARKER, "Total datasets failed: {} ", failedCounter);
  }

  private static void deleteMode() {
    try (Stream<String> stream = Files.lines(Paths.get(DATASET_IDS_PATH))) {
      stream.forEach(datasetId -> {
        deletedCounter++;
        datasetDao.deleteByDatasetId(Integer.parseInt(datasetId));
      });
    } catch (IOException e) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "Reading dataset ids file failed ", e);
    }
    LOGGER.error(EXECUTION_LOGS_MARKER, "Total datasets deleted: {} ", deletedCounter);
  }

  private static void convertLineToDataset(String[] line, DatasetDao datasetDao) {
    Dataset dataset;
    try {
      dataset = extractColumnsFromArray(line);
      Dataset storedDataset = null;
      String datasetIdString = null;
      if (dataset != null) {
        storedDataset = datasetDao.getDatasetByDatasetId(dataset.getDatasetId());
        datasetIdString = Integer.toString(dataset.getDatasetId());
      }

      if (dataset != null && storedDataset == null) {
        datasetDao.create(dataset);
        storedCounter++;
        LOGGER.info(EXECUTION_LOGS_MARKER, "Dataset with datasetId: {}, created", datasetIdString);
        LOGGER.info(SUCCESSFULL_DATASET_IDS, datasetIdString);
      } else if (storedDataset != null) {
        LOGGER.warn(EXECUTION_LOGS_MARKER, "Dataset with datasetId: {}, already exists",
            datasetIdString);
      } else {
        LOGGER.warn(EXECUTION_LOGS_MARKER, "Failed to read dataset from csv line.");
        LOGGER.warn(FAILED_CSV_LINES_MARKER, Arrays.toString(line));
        failedCounter++;
      }
    } catch (ParseException e) {
      LOGGER.warn(EXECUTION_LOGS_MARKER, "Failed to read dataset from csv line.");
      LOGGER.warn(FAILED_CSV_LINES_MARKER, Arrays.toString(line));
      failedCounter++;
    }
  }

  private static void initializeMongoClient() {
    if (MONGO_HOSTS.length != MONGO_PORTS.length && MONGO_PORTS.length != 1) {
      throw new IllegalArgumentException("Mongo hosts and ports are not properly configured.");
    }

    List<ServerAddress> serverAddresses = new ArrayList<>();
    for (int i = 0; i < MONGO_HOSTS.length; i++) {
      ServerAddress address;
      if (MONGO_HOSTS.length == MONGO_PORTS.length) {
        address = new ServerAddress(MONGO_HOSTS[i], MONGO_PORTS[i]);
      } else { // Same port for all
        address = new ServerAddress(MONGO_HOSTS[i], MONGO_PORTS[0]);
      }
      serverAddresses.add(address);
    }

    MongoClientOptions.Builder optionsBuilder = new Builder();
    optionsBuilder.sslEnabled(MONGO_ENABLESSL);
    if (StringUtils.isEmpty(MONGO_DB) || StringUtils.isEmpty(MONGO_USERNAME)
        || StringUtils.isEmpty(MONGO_PASSWORD)) {
      mongoClient = new MongoClient(serverAddresses, optionsBuilder.build());
    } else {
      MongoCredential mongoCredential = MongoCredential
          .createCredential(MONGO_USERNAME, MONGO_AUTHENTICATION_DB, MONGO_PASSWORD.toCharArray());
      mongoClient = new MongoClient(serverAddresses, mongoCredential, optionsBuilder.build());
    }
  }

  private static Dataset extractColumnsFromArray(String[] line) throws ParseException {
    Dataset dataset = new Dataset();
    dataset.setEcloudDatasetId(String.format("NOT_CREATED_YET-%s", UUID.randomUUID().toString()));
    //Only get the first numeric part of the Columns.NAME field
    Pattern pattern = Pattern.compile("^(\\d+)_.*");
    Matcher matcher = pattern.matcher(line[Columns.NAME.getIndex()].trim());
    if (matcher.find()) {
      String datasetId = matcher.group(1);
      dataset.setDatasetId(Integer.parseInt(datasetId));
    } else {
      return null;
    }
    dataset.setDatasetName(line[Columns.NAME.getIndex()].trim());
    dataset.setOrganizationId(ORGANIZATION_ID);
    dataset.setOrganizationName(ORGANIZATION_NAME);
    dataset.setCreatedByUserId(USER_ID);
    SimpleDateFormat dateFormat = new SimpleDateFormat(SUGARCRM_DATE_FORMAT);
    dataset.setCreatedDate(dateFormat.parse(line[Columns.DATE_CREATED.getIndex()].trim()));
    dataset.setCountry(
        Country.getCountryFromIsoCode(line[Columns.DATASET_COUNTRY_CODE.getIndex()].trim()));
    dataset.setDescription(line[Columns.DESCRIPTION.getIndex()]);
    dataset.setNotes(line[Columns.NOTES.getIndex()]);

    if (line[Columns.HARVEST_TYPE.getIndex()].trim().equals("oai_pmh")) {
      OaipmhHarvestPluginMetadata oaipmhHarvestPluginMetadata = new OaipmhHarvestPluginMetadata();
      oaipmhHarvestPluginMetadata.setUrl(line[Columns.HARVEST_URL.getIndex()].trim());
      oaipmhHarvestPluginMetadata
          .setMetadataFormat(line[Columns.METADATA_FORMAT.getIndex()].trim());
      oaipmhHarvestPluginMetadata
          .setSetSpec(line[Columns.SETSPEC.getIndex()].trim().equals("-") ? null
              : line[Columns.SETSPEC.getIndex()].trim());
      oaipmhHarvestPluginMetadata.setMocked(false);
      dataset.setHarvestingMetadata(oaipmhHarvestPluginMetadata);
    }
    return dataset;
  }

}
