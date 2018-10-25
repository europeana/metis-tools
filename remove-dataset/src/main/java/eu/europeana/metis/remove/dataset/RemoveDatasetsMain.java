package eu.europeana.metis.remove.dataset;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.remove.dataset.utils.MongoInitializer;
import eu.europeana.metis.remove.dataset.utils.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveDatasetsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveDatasetsMain.class);

  private static final String DATASET_IDS_FILE = "/home/jochen/Desktop/invalid_dataset_ids.log";

  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main(String[] args)
      throws IOException, MCSException, TrustStoreConfigurationException {

    final List<String> datasetIds = FileUtils
        .readLines(new File(DATASET_IDS_FILE), StandardCharsets.UTF_8).stream()
        .filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toList());

    final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }

    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    DataSetServiceClient datasetServiceClient = new DataSetServiceClient(
        propertiesHolder.ecloudMcsBaseUrl, propertiesHolder.ecloudUsername,
        propertiesHolder.ecloudPassword);

    RecordServiceClient recordServiceClient = new RecordServiceClient(
        propertiesHolder.ecloudMcsBaseUrl, propertiesHolder.ecloudUsername,
        propertiesHolder.ecloudPassword);

    final DatasetRemover datasetRemover = new DatasetRemover(morphiaDatastoreProvider,
        datasetServiceClient, recordServiceClient,
        propertiesHolder.ecloudProvider);

    int count = 0;
    for (String datasetId : datasetIds) {
      count++;
      LOGGER.info("Removing dataset {} of {}.", count, datasetIds.size());
      datasetRemover.removeDataset(datasetId);
    }

    mongoInitializer.close();
  }
}
