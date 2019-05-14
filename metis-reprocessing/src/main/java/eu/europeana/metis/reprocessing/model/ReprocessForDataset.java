package eu.europeana.metis.reprocessing.model;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.utilities.MongoDao;
import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessForDataset implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReprocessForDataset.class);
  private final String datasetId;
  private MongoDao mongoDao;

  public ReprocessForDataset(String datasetId, MongoDao mongoDao) {
    this.datasetId = datasetId;
    this.mongoDao = mongoDao;
  }

  @Override
  public Void call() {
    return reprocessDataset();
  }

  private Void reprocessDataset() {
    LOGGER.info("Processing DatasetId: {} started", datasetId);
    //Code here
    String dsId = "03915";
    final long startTime = System.nanoTime();
    List<FullBeanImpl> nextPageOfRecords = mongoDao.getNextPageOfRecords(dsId, 0);
//    List<FullBeanImpl> nextPageOfRecords = new ArrayList<>();
//    for (int i = 0; i < 1000; i++) {
//      nextPageOfRecords.add(mongoDao.getRecord(
//          "/03915/public_mistral_memoire_fr_ACTION_CHERCHER_FIELD_1_REF_VALUE_1_AP70L00682F"));
//    }
    System.out.println(nextPageOfRecords.size());
    final long endTime = System.nanoTime();
    System.out.println(String
        .format("Total time: %s DatasetId: %s ", (double) (endTime - startTime) / 1_000_000_000.0,
            datasetId));
    LOGGER.info("Processing DatasetId: {} end", datasetId);
    return null;
  }

}
