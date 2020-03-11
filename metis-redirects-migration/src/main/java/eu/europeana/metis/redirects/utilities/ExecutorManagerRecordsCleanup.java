package eu.europeana.metis.redirects.utilities;

import dev.morphia.query.FindOptions;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.mongo.RecordRedirect;
import eu.europeana.metis.mongo.RecordRedirectDao;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-03-03
 */
public class ExecutorManagerRecordsCleanup {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManagerRecordsCleanup.class);
  public static final int SLEEP_SECONDS = 60;
  private final int rowsPerRequest;
  private final List<String> datasetIdsToKeep;
  private final EdmMongoServerImpl edmMongoServer;
  private final RecordRedirectDao recordRedirectDao;
  private Map<String, Set<String>> deadRedirectsPerDatasetId = new HashMap<>();

  public ExecutorManagerRecordsCleanup(EdmMongoServerImpl edmMongoServer,
      RecordRedirectDao recordRedirectDao, int rowsPerRequest, List<String> datasetIdsToKeep) {
    this.rowsPerRequest = rowsPerRequest <= 0 ? 100 : rowsPerRequest;
    this.edmMongoServer = edmMongoServer;
    this.recordRedirectDao = recordRedirectDao;
    this.datasetIdsToKeep = datasetIdsToKeep;
  }

  public void cleanupDatabaseRedirects() throws InterruptedException {
    final dev.morphia.query.Query<RecordRedirect> recordRedirectsQuery = recordRedirectDao
        .getDatastore().createQuery(RecordRedirect.class);

    int nextPage = 0;
    List<RecordRedirect> nextPageResults;
    do {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Parsing page {} - redirect cases till now {}", nextPage, nextPage * rowsPerRequest);
      nextPageResults = getNextPageResults(recordRedirectsQuery, nextPage);
      checkRedirectsHealth(nextPageResults);
      if (nextPage != 0 && nextPage % 1000 == 0) {
        displayCollectedResults();
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Starting sleep to give gc time for {} seconds", SLEEP_SECONDS);
        Thread.sleep(Duration.ofSeconds(SLEEP_SECONDS).toMillis());
      }
      nextPage++;
    } while (CollectionUtils.isNotEmpty(nextPageResults));
//    deleteCollectedDeadRedirects(deadRedirects);

  }

  private void deleteCollectedDeadRedirects() {
    final Iterator<Entry<String, Set<String>>> iterator = deadRedirectsPerDatasetId.entrySet()
        .iterator();

    while (iterator.hasNext()) {
      final Entry<String, Set<String>> datasetIdRedirectsEntrySet = iterator.next();
      LOGGER.info("Removing for datasetId {}", datasetIdRedirectsEntrySet.getKey());
      datasetIdRedirectsEntrySet.getValue().stream().map(id -> recordRedirectDao.getDatastore()
          .createQuery(RecordRedirect.class).field("newId").equal(id))
          .forEach(recordRedirectDao.getDatastore()::delete);
    }
    iterator.remove();
  }

  private <T> List<T> getNextPageResults(dev.morphia.query.Query<T> query, int nextPage) {
    final FindOptions findOptions = new FindOptions().skip(nextPage * rowsPerRequest)
        .limit(rowsPerRequest);
    return ExternalRequestUtil
        .retryableExternalRequestConnectionReset(() -> query.asList(findOptions));
  }

  public void checkRedirectsHealth(List<RecordRedirect> nextPageResults) {
    for (RecordRedirect recordRedirect : nextPageResults) {
      final Query<FullBeanImpl> recordsQuery = edmMongoServer.getDatastore()
          .createQuery(FullBeanImpl.class);
      final String datasetId;

      datasetId = "".equals(recordRedirect.getNewId()) ? "" : recordRedirect.getNewId()
          .substring(1, recordRedirect.getNewId().lastIndexOf('/'));

      //Skip records that are part of the datasetIds that we want to keep in the redirects
      //Also skip records that are already marked as dead
      final Set<String> datasetIdRedirects = deadRedirectsPerDatasetId.get(datasetId);
      if (datasetIdsToKeep.contains(datasetId) || (datasetIdRedirects != null
          && datasetIdRedirects.contains(recordRedirect.getNewId()))) {
        continue;
      }

      //Check if record with newId exists in the record database
      if (CollectionUtils.isEmpty(recordsQuery.field("about").equal(recordRedirect.getNewId())
          .project("about", true).asList())) {
        //Since it doesn't exist we mark the redirect as dead
        Set<String> deadRedirectsOfDatasetId = deadRedirectsPerDatasetId
            .getOrDefault(datasetId, new HashSet<>());
        deadRedirectsOfDatasetId.add(recordRedirect.getNewId());
        deadRedirectsPerDatasetId.putIfAbsent(datasetId, deadRedirectsOfDatasetId);
      }
    }
  }

  public void displayCollectedResults() {
    final int totalDeadRedirects = deadRedirectsPerDatasetId.values().stream().mapToInt(Set::size)
        .sum();
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Display information of collected results");
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total dead redirects: {}",
        totalDeadRedirects);
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total dead datasetId redirect maps: {}",
        deadRedirectsPerDatasetId.size());
    deadRedirectsPerDatasetId.forEach((key, value) -> LOGGER
        .info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total dead redirects for datasetId {}: {}",
            key, value.size()));
  }

}
