package eu.europeana.metis.export.dataset.info;

import com.opencsv.CSVWriter;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.rest.ResponseListWrapper;
import eu.europeana.metis.core.workflow.OrderField;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportDatasetInfoMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExportDatasetInfoMain.class);

  private static final int DATASETS_PER_REQUEST = 20;

  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {

    // Read properties
    final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

    // Load trust store
    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }

    // Load datasets
    LOGGER.info("Loading datasets:");
    final MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    final MorphiaDatastoreProvider datastoreProvider = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);
    final List<Dataset> datasets = getAllDatasets(datastoreProvider);
    LOGGER.info("{} datasets loaded.", datasets.size());
    mongoInitializer.close();

    // Write to the CSV file
    LOGGER.info("Writing to output file: {}.", propertiesHolder.targetFile);
    try (
        final Writer writer = Files.newBufferedWriter(Paths.get(propertiesHolder.targetFile));
        final CSVWriter csvWriter = new CSVWriter(writer,
            CSVWriter.DEFAULT_SEPARATOR,
            CSVWriter.DEFAULT_QUOTE_CHARACTER,
            CSVWriter.DEFAULT_ESCAPE_CHARACTER,
            CSVWriter.DEFAULT_LINE_END)
    ) {
      csvWriter.writeNext(
          new String[]{"Dataset ID", "Dataset name", "Data provider", "Provider", "Country",
              "Link to dataset"});
      for (Dataset dataset : datasets) {
        csvWriter.writeNext(createFullReportLine(dataset, propertiesHolder));
      }
    }
  }

  private static String[] createFullReportLine(Dataset dataset, PropertiesHolder propertiesHolder)
      throws UnsupportedEncodingException {

    // Create result
    final String[] result = new String[6];
    Arrays.fill(result, "");

    // set ID and name
    result[0] = dataset.getDatasetId();
    result[1] = dataset.getDatasetName();

    // Set data provider, provider and country
    if (dataset.getDataProvider() != null) {
      result[2] = dataset.getDataProvider();
    }
    if (dataset.getProvider() != null) {
      result[3] = dataset.getProvider();
    }
    if (dataset.getCountry() != null && dataset.getCountry().getName() != null) {
      result[4] = dataset.getCountry().getName();
    }

    // Set Metis link
    result[5] = propertiesHolder.metisDatasetUriPrefix + URLEncoder.encode(dataset.getDatasetId(),
        StandardCharsets.UTF_8.name());

    // Done
    return result;
  }

  private static List<Dataset> getAllDatasets(MorphiaDatastoreProvider datastoreProvider) {
    final List<Dataset> result = new ArrayList<>();
    int nextPage = 0;
    do {
      final ResponseListWrapper<Dataset> responseListWrapper = new ResponseListWrapper<>();
      responseListWrapper
          .setResultsAndLastPage(getDatasetPage(datastoreProvider, nextPage), DATASETS_PER_REQUEST,
              nextPage);
      LOGGER
          .info("  Next page: {}, {} datasets found.", nextPage, responseListWrapper.getListSize());
      nextPage = responseListWrapper.getNextPage();
      result.addAll(responseListWrapper.getResults());
    } while (nextPage != -1);
    return result;
  }

  private static List<Dataset> getDatasetPage(MorphiaDatastoreProvider datastoreProvider,
      int page) {
    final Query<Dataset> query = datastoreProvider.getDatastore().createQuery(Dataset.class);
    query.order(OrderField.ID.getOrderFieldName());
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(() -> query.asList(
        new FindOptions().skip(page * DATASETS_PER_REQUEST).limit(DATASETS_PER_REQUEST)));
  }
}
