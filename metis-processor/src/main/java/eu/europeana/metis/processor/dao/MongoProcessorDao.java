package eu.europeana.metis.processor.dao;

import com.mongodb.client.MongoClient;
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

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

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

    private MongoClient initializeMongoClient() throws DataAccessConfigException {
        return new MongoClientProvider<>(mongoProcessorProperties.getMongoProcessorProperties()).createMongoClient();
    }

    private Datastore initializeDatastore() {
        final Datastore datastore = Morphia.createDatastore(mongoClient, mongoProcessorProperties.getMongoProcessorDatabase());
        final Mapper mapper = datastore.getMapper();
        mapper.map(DatasetStatus.class);
        datastore.ensureIndexes();
        return datastore;
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

    public DatasetPageBuilder getNextDatasetPageNumber(int recordPageSize) {
        Query<DatasetStatus> query = metisProcessorDatastore.find(DatasetStatus.class);
        query.filter(Filters.expr(Expressions.value("{$lt: [\"$totalProcessed\", \"$totalRecords\"]}")));
        List<DatasetStatus> datasetStatuses = MorphiaUtils.getListOfQueryRetryable(query);

        DatasetPageBuilder datasetPageBuilder = new DatasetPageBuilder(null, -1);
        // TODO: 21/08/2023 Remove this, only for testing.
//        datasetStatuses = datasetStatuses.stream()
//                .filter(datasetStatus -> datasetStatus.getDatasetId().equals("2048087")).collect(Collectors.toList());
        for (DatasetStatus datasetStatus : datasetStatuses) {
            final SortedSet<Integer> pagesProcessed = new TreeSet<>(datasetStatus.getPagesProcessed());
            final SortedSet<Integer> currentPagesProcessing = new TreeSet<>(datasetStatus.getCurrentPagesProcessing());
            if (pagesProcessed.isEmpty()) {
                datasetStatus.getCurrentPagesProcessing().add(0);
                datasetPageBuilder = new DatasetPageBuilder(datasetStatus.getDatasetId(), 0);
            } else {
                //Possible total pages Math.ceil((float)processedRecords/pageSize)
                int totalPages = (int) Math.ceil((float) datasetStatus.getTotalRecords() / recordPageSize);
                //Ensure that if a page failed in the meantime, we don't get stuck repeating the last page.
                boolean isLastPageProcessed = pagesProcessed.contains(totalPages - 1);
                int nextPage = Math.max(pagesProcessed.last(), currentPagesProcessing.isEmpty() ? Integer.MIN_VALUE : currentPagesProcessing.last()) + 1;
                if (nextPage < totalPages && !isLastPageProcessed) {
                    datasetStatus.getCurrentPagesProcessing().add(nextPage);
                    datasetPageBuilder = new DatasetPageBuilder(datasetStatus.getDatasetId(), nextPage);
                }
            }
            if(datasetPageBuilder.getDatasetId() != null){
                metisProcessorDatastore.save(datasetStatuses);
                break;
            }
        }
        return datasetPageBuilder;
    }

}
