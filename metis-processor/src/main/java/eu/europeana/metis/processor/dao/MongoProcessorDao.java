package eu.europeana.metis.processor.dao;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.SocketSettings;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.aggregation.expressions.Expressions;
import dev.morphia.mapping.Mapper;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.properties.mongo.MongoProcessorProperties;
import eu.europeana.metis.processor.utilities.DatasetPage.DatasetPageBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;

public class MongoProcessorDao {

  private static final String DATASET_ID = "datasetId";

  private final MongoProcessorProperties mongoProcessorProperties;
  private final MongoClient mongoClient;
  private final Datastore metisProcessorDatastore;

  public MongoProcessorDao(MongoProcessorProperties mongoProcessorProperties) throws DataAccessConfigException {
    this.mongoProcessorProperties = mongoProcessorProperties;
    this.mongoClient = initializeMongoClient();
    this.metisProcessorDatastore = initializeDatastore();
  }

  public List<DatasetStatus> getAllDatasetStatuses() {
    return MorphiaUtils.getListOfQueryRetryable(metisProcessorDatastore.find(DatasetStatus.class));
  }

  public List<FailedEnhancementRecord> getAllFailedEnhancementRecords() {
    return MorphiaUtils.getListOfQueryRetryable(metisProcessorDatastore.find(FailedEnhancementRecord.class));
  }

  public DatasetStatus getDatasetStatus(String datasetId) {
    return metisProcessorDatastore.find(DatasetStatus.class).filter(Filters.eq(DATASET_ID, datasetId)).first();
  }

  public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> metisProcessorDatastore.save(datasetStatus));
  }

  public DatasetPageBuilder getNextDatasetPageNumber(String nextPageId, int recordPageSize) {
    Query<DatasetStatus> query = metisProcessorDatastore.find(DatasetStatus.class);
    query.filter(Filters.expr(Expressions.value("{$lt: [\"$totalProcessed\", \"$totalRecords\"]}")));
    List<DatasetStatus> datasetStatuses = MorphiaUtils.getListOfQueryRetryable(query);

    DatasetPageBuilder datasetPageBuilder = new DatasetPageBuilder(null, new ObjectId("000000000000000000000000"));
    for (DatasetStatus datasetStatus : datasetStatuses) {
      final SortedSet<ObjectId> pagesProcessed = new TreeSet<>(datasetStatus.getPagesProcessed());
      final SortedSet<ObjectId> currentPagesProcessing = new TreeSet<>(datasetStatus.getCurrentPagesProcessing());
      if (pagesProcessed.isEmpty()) {
        datasetStatus.getCurrentPagesProcessing().add(new ObjectId(nextPageId));
        datasetPageBuilder = new DatasetPageBuilder(datasetStatus.getDatasetId(), new ObjectId("000000000000000000000000"));
      } else {
        //Possible total pages Math.ceil((float)processedRecords/pageSize)
        int totalPages = (int) Math.ceil((float) datasetStatus.getTotalRecords() / recordPageSize);
        //Ensure that if a page failed in the meantime, we don't get stuck repeating the last page.
        boolean isLastPageProcessed = pagesProcessed.contains(totalPages - 1);
        ObjectId nextPage = getNextObjectId(getMaxObjectId(pagesProcessed.last(),
            currentPagesProcessing.isEmpty() ? new ObjectId("000000000000000000000000") : currentPagesProcessing.last()));
        if (!isLastPageProcessed) {
          datasetStatus.getCurrentPagesProcessing().add(new ObjectId(nextPageId));
          datasetPageBuilder = new DatasetPageBuilder(datasetStatus.getDatasetId(), nextPage);
        }
      }
      if (datasetPageBuilder.getDatasetId() != null) {
        metisProcessorDatastore.save(datasetStatuses);
        break;
      }
    }
    return datasetPageBuilder;
  }

  private MongoClient initializeMongoClient() throws DataAccessConfigException {
    // Configure SocketSettings with a 16 MB buffer for send and receive
    SocketSettings socketSettings = SocketSettings.builder()
                                                  .receiveBufferSize(16 * 1024 * 1024) // 16 MB buffer for receiving
                                                  .sendBufferSize(16 * 1024 * 1024)    // 16 MB buffer for sending
                                                  .connectTimeout(30, TimeUnit.SECONDS) // Connection timeout
                                                  .readTimeout(30, TimeUnit.SECONDS)    // Read timeout
                                                  .build();
    MongoClientSettings.Builder settings = MongoClientSettings.builder()
                                                              .applyToSocketSettings(
                                                                  builder -> builder.applySettings(socketSettings))
                                                              .compressorList(
                                                                  Arrays.asList(MongoCompressor.createSnappyCompressor(),
                                                                      MongoCompressor.createZlibCompressor(),
                                                                      MongoCompressor.createZstdCompressor()));
    return new MongoClientProvider<>(mongoProcessorProperties.getMongoProcessorProperties(), settings).createMongoClient();
  }

  private Datastore initializeDatastore() {
    final Datastore datastore = Morphia.createDatastore(mongoClient, mongoProcessorProperties.getMongoProcessorDatabase());
    final Mapper mapper = datastore.getMapper();
    mapper.map(DatasetStatus.class);
    datastore.ensureIndexes();
    return datastore;
  }

  static ObjectId getMaxObjectId(ObjectId id1, ObjectId id2) {
    return id1.compareTo(id2) >= 0 ? id1 : id2;
  }

  static ObjectId getNextObjectId(ObjectId objectId) {
    // Convert the ObjectId to its byte array representation
    byte[] bytes = objectId.toByteArray();

    // Increment the byte array as a big-endian integer
    for (int i = bytes.length - 1; i >= 0; i--) {
      bytes[i]++;
      if (bytes[i] != 0) { // No overflow, stop incrementing
        break;
      }
    }

    // Create a new ObjectId from the incremented byte array
    return new ObjectId(bytes);
  }
  //    public DatasetPageBuilder getNextDatasetPageNumber(int recordPageSize) {
  //        Query<DatasetStatus> query = metisProcessorDatastore.find(DatasetStatus.class);
  //        query.filter(Filters.expr(Expressions.value("{$lt: [\"$totalProcessed\", \"$totalRecords\"]}")));
  //        List<DatasetStatus> datasetStatuses = MorphiaUtils.getListOfQueryRetryable(query);
  //
  //        DatasetPageBuilder datasetPageBuilder = new DatasetPageBuilder(null, -1);
  //        // TODO: 21/08/2023 Remove this, only for testing.
  ////        datasetStatuses = datasetStatuses.stream()
  ////                .filter(datasetStatus -> datasetStatus.getDatasetId().equals("2048087")).collect(Collectors.toList());
  //        for (DatasetStatus datasetStatus : datasetStatuses) {
  //            final SortedSet<Integer> pagesProcessed = new TreeSet<>(datasetStatus.getPagesProcessed());
  //            final SortedSet<Integer> currentPagesProcessing = new TreeSet<>(datasetStatus.getCurrentPagesProcessing());
  //            if (pagesProcessed.isEmpty()) {
  //                datasetStatus.getCurrentPagesProcessing().add(0);
  //                datasetPageBuilder = new DatasetPageBuilder(datasetStatus.getDatasetId(), 0);
  //            } else {
  //                //Possible total pages Math.ceil((float)processedRecords/pageSize)
  //                int totalPages = (int) Math.ceil((float) datasetStatus.getTotalRecords() / recordPageSize);
  //                //Ensure that if a page failed in the meantime, we don't get stuck repeating the last page.
  //                boolean isLastPageProcessed = pagesProcessed.contains(totalPages - 1);
  //                int nextPage = Math.max(pagesProcessed.last(), currentPagesProcessing.isEmpty() ? Integer.MIN_VALUE : currentPagesProcessing.last()) + 1;
  //                if (nextPage < totalPages && !isLastPageProcessed) {
  //                    datasetStatus.getCurrentPagesProcessing().add(nextPage);
  //                    datasetPageBuilder = new DatasetPageBuilder(datasetStatus.getDatasetId(), nextPage);
  //                }
  //            }
  //            if(datasetPageBuilder.getDatasetId() != null){
  //                metisProcessorDatastore.save(datasetStatuses);
  //                break;
  //            }
  //        }
  //        return datasetPageBuilder;
  //    }

}
