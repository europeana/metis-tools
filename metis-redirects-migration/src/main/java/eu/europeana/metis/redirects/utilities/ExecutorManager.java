package eu.europeana.metis.redirects.utilities;

import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.corelib.tools.lookuptable.EuropeanaId;
import eu.europeana.metis.mongo.RecordRedirect;
import eu.europeana.metis.mongo.RecordRedirectDao;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2020-02-25
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private final int rowsPerRequest;
  private final EdmMongoServerImpl edmMongoServer;
  private final RecordRedirectDao recordRedirectDao;
  private final Map<String, Date> oldIdsDateMap = new HashMap<>();
  private final List<LinkedList<String>> listOfChains = new ArrayList<>();
  private final List<EuropeanaId> selfRedirects = new ArrayList<>();
  private final List<LinkedList<String>> circularRedirectChains = new ArrayList<>();

  private int totalListOfChains = 0;
  private Map<Integer, Integer> totalChainSizeBasedMapOrdered = new LinkedHashMap<>();

  public ExecutorManager(EdmMongoServerImpl edmMongoServer, RecordRedirectDao recordRedirectDao,
      int rowsPerRequest) {
    this.rowsPerRequest = rowsPerRequest <= 0 ? 100 : rowsPerRequest;
    this.edmMongoServer = edmMongoServer;
    this.recordRedirectDao = recordRedirectDao;
  }

  public void analyzeOldDatabaseRedirects() {
    Query<EuropeanaId> europeanaIdQuery = edmMongoServer.getDatastore()
        .createQuery(EuropeanaId.class);
    final long count = europeanaIdQuery.count();
    LOGGER
        .info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total rows of redirects in the database: {}",
            count);

    int nextPage = 0;
    List<EuropeanaId> nextPageResults;
    do {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Parsing page {}", nextPage);
      europeanaIdQuery = edmMongoServer.getDatastore().createQuery(EuropeanaId.class);
      nextPageResults = getNextPageResults(europeanaIdQuery, nextPage);
      analyzeRows(nextPageResults);
      if (nextPage % 10 == 0) {
        displayCollectedResults();
        tranferMemoryResultsIntoNewDb();
      }
      nextPage++;
    } while (!CollectionUtils.isEmpty(nextPageResults));
  }

  private <T> List<T> getNextPageResults(Query<T> query, int nextPage) {
    final FindOptions findOptions = new FindOptions().skip(nextPage * rowsPerRequest)
        .limit(rowsPerRequest);
    return ExternalRequestUtil
        .retryableExternalRequestConnectionReset(() -> query.asList(findOptions));
  }

  private void analyzeRows(List<EuropeanaId> nextPageResults) {
    for (EuropeanaId europeanaId : nextPageResults) {
      boolean europeanaIdAcceptable = true;
      if (europeanaId.getOldId().equals(europeanaId.getNewId())) {
        //Ignore the self references
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Self reference detected with oldId {} and newId {}", europeanaId.getOldId(),
            europeanaId.getNewId());
        selfRedirects.add(europeanaId);
        europeanaIdAcceptable = false;
      }
      //The oldIds map may contain items that are must not have been stored in the new database
      //because they should be invalid, therefore we still want to bypass them
      if (isRedirectAlreadyMigrated(europeanaId.getOldId()) || oldIdsDateMap
          .containsKey(europeanaId.getOldId())) {
        //If already encountered, no need to further do anything
        LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Already encountered oldId, bypassing oldId {}, newId {}", europeanaId.getOldId(),
            europeanaId.getNewId());
        europeanaIdAcceptable = false;
      }

      if (europeanaIdAcceptable) {
        final LinkedList<String> chain = new LinkedList<>();
        chain.addFirst(europeanaId.getOldId());
        chain.addLast(europeanaId.getNewId());
        //Add to encounters for faster reach on subsequent page results
        oldIdsDateMap.put(europeanaId.getOldId(), new Date(europeanaId.getTimestamp()));

        if (populateChainBackwards(chain)) {
          populateChainForwards(chain);
          listOfChains.add(chain);
        }
      }
    }
  }

  private boolean populateChainBackwards(LinkedList<String> chain) {
    String identifier = chain.getFirst();
    List<EuropeanaId> europeanaIds;
    do {
      Query<EuropeanaId> europeanaIdQuery = edmMongoServer.getDatastore()
          .createQuery(EuropeanaId.class);
      europeanaIds = europeanaIdQuery.field("newId").equal(identifier).asList();
      if (!CollectionUtils.isEmpty(europeanaIds)) {
        //Get the latest, based on timestamp
        final List<EuropeanaId> sortedEuropeanaIds = europeanaIds.stream()
            .sorted(Comparator.comparing(EuropeanaId::getTimestamp)).collect(
                Collectors.toList());
        final EuropeanaId latestEuropeanaId = sortedEuropeanaIds.get(sortedEuropeanaIds.size() - 1);
        if (chain.contains(latestEuropeanaId.getOldId())) {
          //We have encountered a loop.
          LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER,
              "Circular chain detected with oldId: {}", latestEuropeanaId.getOldId());
          oldIdsDateMap
              .put(latestEuropeanaId.getOldId(), new Date(latestEuropeanaId.getTimestamp()));
          circularRedirectChains.add(chain);
          return false;
        }
        chain.addFirst(latestEuropeanaId.getOldId());
        identifier = latestEuropeanaId.getOldId();
        oldIdsDateMap
            .put(latestEuropeanaId.getOldId(), new Date(latestEuropeanaId.getTimestamp()));

        //Retrieve older redirections and add them to encounters
        if (sortedEuropeanaIds.size() - 1 > 0) {
          LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER,
              "Results more that one for newId: {}, parsing older chain and adding them to encounters",
              latestEuropeanaId.getNewId());
          for (int i = 0; i < sortedEuropeanaIds.size() - 1; i++) {
            parseChainTillBeginning(europeanaIds.get(i));
          }
          LOGGER
              .warn(PropertiesHolder.EXECUTION_LOGS_MARKER, "Finished parsing chain for newId: {}",
                  latestEuropeanaId.getNewId());
        }
      }
    } while (!CollectionUtils.isEmpty(europeanaIds));
    return true;
  }

  private void parseChainTillBeginning(EuropeanaId europeanaId) {
    List<EuropeanaId> europeanaIds;
    oldIdsDateMap.put(europeanaId.getOldId(), new Date(europeanaId.getTimestamp()));
    do {
      Query<EuropeanaId> europeanaIdQuery = edmMongoServer.getDatastore()
          .createQuery(EuropeanaId.class);
      europeanaIds = europeanaIdQuery.field("newId").equal(europeanaId)
          .asList();
      europeanaIds.forEach(this::parseChainTillBeginning);
    } while (!CollectionUtils.isEmpty(europeanaIds));
  }

  private void populateChainForwards(LinkedList<String> chain) {
    String identifier = chain.getLast();
    List<EuropeanaId> europeanaIds;
    do {
      Query<EuropeanaId> europeanaIdQuery = edmMongoServer.getDatastore()
          .createQuery(EuropeanaId.class);
      //Result can only be null or one because oldId is a unique index
      europeanaIds = europeanaIdQuery.field("oldId").equal(identifier).asList();
      if (!CollectionUtils.isEmpty(europeanaIds)) {
        chain.addLast(europeanaIds.get(0).getNewId());
        identifier = europeanaIds.get(0).getNewId();
        oldIdsDateMap
            .put(europeanaIds.get(0).getOldId(), new Date(europeanaIds.get(0).getTimestamp()));
      }
    } while (!CollectionUtils.isEmpty(europeanaIds));
  }

  public void displayCollectedResults() {
    //Calculates and adds up the statistics to the totals
    final Map<Integer, List<LinkedList<String>>> chainSizeBasedMapOrdered = listOfChains.stream()
        .collect(Collectors.groupingBy(LinkedList::size)).entrySet().stream()
        .sorted(Entry.comparingByKey())
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1,
            LinkedHashMap::new));

    totalListOfChains += listOfChains.size();
    chainSizeBasedMapOrdered.forEach((key, value) -> {
      Integer totalChainsOfKeyLength = totalChainSizeBasedMapOrdered.getOrDefault(key, 0);
      totalChainsOfKeyLength += value.size();
      totalChainSizeBasedMapOrdered.put(key, totalChainsOfKeyLength);
    });

    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Display information of collected results");
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total collected chains: {}", totalListOfChains);
    totalChainSizeBasedMapOrdered.forEach((key, value) -> LOGGER
        .info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total chains of {} redirects per chain are: {}", key - 1, value));
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total collected self redirects: {}", selfRedirects.size());
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total collected circular redirect chains: {}", circularRedirectChains.size());
  }

  public void tranferMemoryResultsIntoNewDb() {
    final ListIterator<LinkedList<String>> listIterator = listOfChains.listIterator();
    while (listIterator.hasNext()) {
      LinkedList<String> chain = listIterator.next();
      final String lastRedirect = chain.getLast();
      String oldId = chain.pollFirst();
      String newId;
      do {
        newId = chain.pollFirst();
        if (newId != null) {
          String sanitizedOldId = oldId;
          if (oldId.startsWith("http://www.europeana.eu/resolve/record")) {
            //Cleanup of old prefix url
            sanitizedOldId = oldId.substring("http://www.europeana.eu/resolve/record".length());
          }
          final RecordRedirect recordRedirect = new RecordRedirect(lastRedirect, sanitizedOldId,
              oldIdsDateMap.get(oldId));
          oldIdsDateMap.remove(oldId);
          recordRedirectDao.createUpdate(recordRedirect);
        }
        oldId = newId;
      } while (newId != null);
      listIterator.remove();
    }
  }

  private boolean isRedirectAlreadyMigrated(String oldId) {
    return !CollectionUtils.isEmpty(recordRedirectDao.getRecordRedirectsByOldId(oldId));
  }

}
