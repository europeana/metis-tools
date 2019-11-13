package eu.europeana.metis.remove.dataset;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.remove.utils.Application;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveDatasetsMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveDatasetsMain.class);

  private static final String DATASET_IDS_FILE = "/home/jochen/Desktop/invalid_dataset_ids.log";

  public static void main(String[] args) throws MCSException, TrustStoreConfigurationException {

    final List<String> datasetIds =
        Arrays.asList(
            // ADD DATASETS HERE.
        );
//        FileUtils.readLines(new File(DATASET_IDS_FILE), StandardCharsets.UTF_8).stream()
//        .filter(StringUtils::isNotBlank).map(String::trim).collect(Collectors.toList());

    try(final Application application = Application.initialize()) {

      DataSetServiceClient datasetServiceClient = new DataSetServiceClient(
          application.getProperties().ecloudMcsBaseUrl, application.getProperties().ecloudUsername,
          application.getProperties().ecloudPassword);

      RecordServiceClient recordServiceClient = new RecordServiceClient(
          application.getProperties().ecloudMcsBaseUrl, application.getProperties().ecloudUsername,
          application.getProperties().ecloudPassword);

      final DatasetRemover datasetRemover = new DatasetRemover(application.getDatastoreProvider(),
          datasetServiceClient, recordServiceClient, application.getProperties().ecloudProvider);

      int count = 0;
      for (String datasetId : datasetIds) {
        count++;
        LOGGER.info("Removing dataset {} of {}.", count, datasetIds.size());
        datasetRemover.removeDataset(datasetId);
      }
    }
  }
}
