package eu.europeana.metis.redirects.utilities;

import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.corelib.tools.lookuptable.EuropeanaId;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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
  private static final int ROWS_PER_REQUEST = 1000;
  private final EdmMongoServerImpl edmMongoServer;
  private final Map<String, EuropeanaId> encounteredRows = new HashMap<>();
  private final List<LinkedList<String>> listOfChains = new ArrayList<>();
  private final List<EuropeanaId> selfRedirects = new ArrayList<>();
  private final List<LinkedList<String>> circularRedirectChains = new ArrayList<>();

  public ExecutorManager(EdmMongoServerImpl edmMongoServer) {
    this.edmMongoServer = edmMongoServer;
  }

  public void analyzeOldDatabaseRedirects() {
    Query<EuropeanaId> europeanaIdQuery = edmMongoServer.getDatastore()
        .createQuery(EuropeanaId.class);
    final long count = europeanaIdQuery.count();
    LOGGER.info("Total rows of redirects in the database: {}", count);

    int nextPage = 0;
    List<EuropeanaId> nextPageResults;
    do {
      LOGGER.info("Parsing page {}", nextPage);
      europeanaIdQuery = edmMongoServer.getDatastore()
          .createQuery(EuropeanaId.class);
      nextPageResults = getNextPageResults(europeanaIdQuery, nextPage);
      analyzeRows(nextPageResults);
      if (nextPage % 10 == 0) {
        displayCollectedResults();
      }
      nextPage++;
    } while (!CollectionUtils.isEmpty(nextPageResults));
  }

  private <T> List<T> getNextPageResults(Query<T> query, int nextPage) {
    final FindOptions findOptions = new FindOptions().skip(nextPage * ROWS_PER_REQUEST)
        .limit(ROWS_PER_REQUEST);
    return ExternalRequestUtil
        .retryableExternalRequestConnectionReset(() -> query.asList(findOptions));
  }

  private void analyzeRows(List<EuropeanaId> nextPageResults) {
    for (EuropeanaId europeanaId : nextPageResults) {
      if (europeanaId.getOldId().equals(europeanaId.getNewId())) {
        //Ignore the self references
        LOGGER.warn("Self reference detected with oldId {} and newId {}", europeanaId.getOldId(),
            europeanaId.getNewId());
        selfRedirects.add(europeanaId);
        continue;
      }
      if (encounteredRows.containsKey(europeanaId.getOldId())) {
        //If already encountered, no need to further do anything
        LOGGER
            .warn("Already encountered oldId, bypassing oldId {}, newId {}", europeanaId.getOldId(),
                europeanaId.getNewId());
        continue;
      }

      final LinkedList<String> chain = new LinkedList<>();
      chain.addFirst(europeanaId.getOldId());
      chain.addLast(europeanaId.getNewId());
      //Add to encounters for faster reach on subsequent page results
      encounteredRows.put(europeanaId.getOldId(), europeanaId);

      if (populateChainBackwards(chain)) {
        populateChainForwards(chain);
        listOfChains.add(chain);
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
          LOGGER.warn("Circular chain detected with oldId: {}", latestEuropeanaId.getOldId());
          circularRedirectChains.add(chain);
          return false;
        }
        chain.addFirst(latestEuropeanaId.getOldId());
        identifier = latestEuropeanaId.getOldId();
        encounteredRows.put(latestEuropeanaId.getOldId(), latestEuropeanaId);

        //Retrieve older redirections and add them to encounters
        for (int i = 0; i < sortedEuropeanaIds.size() - 1; i++) {
          LOGGER.warn(
              "Results more that one for newId: {}, parsing older chain and adding them to encounters",
              latestEuropeanaId.getNewId());
          parseChainTillBeginning(europeanaIds.get(i));
          LOGGER.warn("Finished parsing chain for newId: {}", latestEuropeanaId.getNewId());
        }
      }
    } while (!CollectionUtils.isEmpty(europeanaIds));
    return true;
  }

  private void parseChainTillBeginning(EuropeanaId europeanaId) {
    List<EuropeanaId> europeanaIds;
    encounteredRows.put(europeanaId.getOldId(), europeanaId);
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
        encounteredRows.put(europeanaIds.get(0).getOldId(), europeanaIds.get(0));
      }
    } while (!CollectionUtils.isEmpty(europeanaIds));
  }

  private void displayCollectedResults() {
    final Map<Integer, List<LinkedList<String>>> chainSizeBasedMapOrdered = listOfChains.stream()
        .collect(Collectors.groupingBy(LinkedList::size)).entrySet().stream()
        .sorted(Entry.comparingByKey())
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (v1, v2) -> v1,
            LinkedHashMap::new));

    LOGGER.info("Display information of collected results");
    LOGGER.info("Total collected chains: {}", listOfChains.size());
    chainSizeBasedMapOrdered.forEach((key, value) -> LOGGER
        .info("Total chains of {} redirects per chain are: {}", key - 1, value.size()));
    LOGGER.info("Total collected self redirects: {}", selfRedirects);
    LOGGER.info("Total collected circular redirect chains: {}", circularRedirectChains);
  }

}
