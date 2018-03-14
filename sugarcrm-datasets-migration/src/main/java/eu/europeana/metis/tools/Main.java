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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-13
 */
public class Main {

  private static final String ORGANIZATION_ID;
  private static final String ORGANIZATION_NAME;
  private static final String USER_ID;
  private static final String DATASETS_CSV_PATH;
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
    if (StringUtils.isNotEmpty(TRUSTSTORE_PATH) && StringUtils
        .isNotEmpty(TRUSTSTORE_PASSWORD)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD);
    }
    initializeMongoClient();
    DatasetDao datasetDao = new DatasetDao(new MorphiaDatastoreProvider(mongoClient, MONGO_DB));

    try (CSVReader reader = new CSVReader(new FileReader(DATASETS_CSV_PATH))) {
      String[] line;
      reader.readNext();//Bypass titles line
      while ((line = reader.readNext()) != null) {
        Dataset dataset = extractColumnsFromArray(line);
        datasetDao.create(dataset);
      }
    } catch (IOException | ParseException e) {
      e.printStackTrace();
    }

    mongoClient.close();
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
    dataset
        .setCountry(
            Country.getCountryFromIsoCode(line[Columns.DATASET_COUNTRY_CODE.getIndex()].trim()));
    dataset.setDescription(line[Columns.DESCRIPTION.getIndex()]);
    dataset.setNotes(line[Columns.NOTES.getIndex()]);

    if (line[Columns.HARVEST_TYPE.getIndex()].trim().equals("oai_pmh")) {
      OaipmhHarvestPluginMetadata oaipmhHarvestPluginMetadata = new OaipmhHarvestPluginMetadata();
      oaipmhHarvestPluginMetadata.setUrl(line[Columns.HARVEST_URL.getIndex()].trim());
      oaipmhHarvestPluginMetadata
          .setMetadataFormat(line[Columns.METADATA_FORMAT.getIndex()].trim());
      oaipmhHarvestPluginMetadata.setSetSpec(
          line[Columns.SETSPEC.getIndex()].trim().equals("-") ? null
              : line[Columns.SETSPEC.getIndex()].trim());
      oaipmhHarvestPluginMetadata.setMocked(false);
      dataset.setHarvestingMetadata(oaipmhHarvestPluginMetadata);
    }
    return dataset;
  }

}
