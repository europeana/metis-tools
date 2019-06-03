package eu.europeana.metis.reprocessing.execution;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.AggregationImpl;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-03
 */
public class StatisticsForDataset implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsForDataset.class);
  private final String prefixDatasetidLog;
  private final String datasetId;
  private final BasicConfiguration basicConfiguration;
  private final boolean singlePage;

  public StatisticsForDataset(String datasetId,
      BasicConfiguration basicConfiguration, boolean singlePage) {
    this.datasetId = datasetId;
    this.prefixDatasetidLog = String.format("DatasetId: %s", this.datasetId);
    this.basicConfiguration = basicConfiguration;
    this.singlePage = singlePage;
  }

  @Override
  public Void call() {
    return calculateStatistics();
  }

  private Void calculateStatistics() {
    final long totalRecordsForDataset = basicConfiguration.getMongoSourceMongoDao()
        .getTotalRecordsForDataset(datasetId);
    int nextPage = 0;
    long totalElapsedTime = 0;
    List<FullBeanImpl> nextPageOfRecords;
    AtomicLong totalEuropeanaWebResourcesCounter = new AtomicLong();
    AtomicLong totalProviderWebResourcesCounter = new AtomicLong();
    AtomicLong totalPlacesCounter = new AtomicLong();
    AtomicLong totalAgentsCounter = new AtomicLong();
    AtomicLong totalTimespansCounter = new AtomicLong();
    AtomicLong totalConceptsCounter = new AtomicLong();
    do {
      final long startTimeIndex = System.nanoTime();
      nextPageOfRecords = basicConfiguration.getMongoSourceMongoDao()
          .getNextPageOfRecords(datasetId, nextPage);
      final long endTimeIndex = System.nanoTime();
      final long elapsedTime = endTimeIndex - startTimeIndex;
      totalElapsedTime += elapsedTime;
      nextPageOfRecords.forEach(fullBeanImpl -> {
        totalEuropeanaWebResourcesCounter
            .addAndGet(fullBeanImpl.getEuropeanaAggregation().getWebResources().size());
        totalProviderWebResourcesCounter
            .addAndGet(
                fullBeanImpl.getAggregations().stream().filter(Objects::nonNull).map(
                    AggregationImpl::getWebResources).mapToInt(List::size).sum());
        totalPlacesCounter.addAndGet(fullBeanImpl.getPlaces().size());
        totalAgentsCounter.addAndGet(fullBeanImpl.getAgents().size());
        totalTimespansCounter.addAndGet(fullBeanImpl.getTimespans().size());
        totalConceptsCounter.addAndGet(fullBeanImpl.getConcepts().size());
      });
      nextPage++;
    } while (!singlePage && CollectionUtils.isNotEmpty(nextPageOfRecords));

    generateLogs(totalRecordsForDataset, nextPage, totalElapsedTime,
        totalEuropeanaWebResourcesCounter,
        totalProviderWebResourcesCounter, totalPlacesCounter, totalAgentsCounter,
        totalTimespansCounter,
        totalConceptsCounter);
    return null;
  }

  private void generateLogs(long totalRecordsForDataset, int nextPage, long totalElapsedTime,
      AtomicLong totalEuropeanaWebResourcesCounter, AtomicLong totalProviderWebResourcesCounter,
      AtomicLong totalPlacesCounter, AtomicLong totalAgentsCounter,
      AtomicLong totalTimespansCounter, AtomicLong totalConceptsCounter) {
    long actualTotalRecordsRetrieved = totalRecordsForDataset;
    if (singlePage) {
      actualTotalRecordsRetrieved = totalRecordsForDataset < MongoSourceMongoDao.PAGE_SIZE
          ? totalRecordsForDataset : MongoSourceMongoDao.PAGE_SIZE;
    }

    LOGGER
        .info(
            STATISTICS_LOGS_MARKER,
            "{} - Total Dataset Records: {}, checked records: {}, page reading: {}, EuropeanaWebResources: {}, ProviderWebResources: {}, Places: {}, Agents: {}, Timespans: {}, Concepts: {}",
            prefixDatasetidLog, totalRecordsForDataset, actualTotalRecordsRetrieved,
            nanoTimeToSeconds(totalElapsedTime),
            totalEuropeanaWebResourcesCounter.get(), totalProviderWebResourcesCounter.get(),
            totalPlacesCounter.get(), totalAgentsCounter.get(), totalTimespansCounter.get(),
            totalConceptsCounter.get());

    LOGGER
        .info(
            STATISTICS_LOGS_MARKER,
            "\u001B[34m{} - Average page read: {}, record read: {}, Averages per record EuropeanaWebResources: {}, ProviderWebResources: {}, Places: {}, Agents: {}, Timespans: {}, Concepts: {}\u001B[0m",
            prefixDatasetidLog, nanoTimeToSeconds(totalElapsedTime) / nextPage,
            nanoTimeToSeconds(totalElapsedTime) / actualTotalRecordsRetrieved,
            totalEuropeanaWebResourcesCounter.get() / actualTotalRecordsRetrieved,
            totalProviderWebResourcesCounter.get() / actualTotalRecordsRetrieved,
            totalPlacesCounter.get() / actualTotalRecordsRetrieved,
            totalAgentsCounter.get() / actualTotalRecordsRetrieved,
            totalTimespansCounter.get() / actualTotalRecordsRetrieved,
            totalConceptsCounter.get() / actualTotalRecordsRetrieved);
  }

  private double nanoTimeToSeconds(long nanoTime) {
    return nanoTime / 1_000_000_000.0;
  }


}
