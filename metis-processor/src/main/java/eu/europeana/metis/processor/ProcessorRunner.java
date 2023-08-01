package eu.europeana.metis.processor;

import com.mongodb.MongoWriteException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.properties.general.ApplicationProperties;
import eu.europeana.metis.processor.dao.*;
import eu.europeana.metis.processor.utilities.DatasetPage;
import eu.europeana.metis.processor.utilities.DatasetPage.DatasetPageBuilder;
import eu.europeana.metis.schema.jibx.EdmType;
import eu.europeana.metis.schema.jibx.RDF;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

// TODO: 24/07/2023 Implement reprocessing of failed pages
public class ProcessorRunner implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String DATASET_STATUS = "datasetStatus";
    private final MongoProcessorDao mongoProcessorDao;
    private final MongoCoreDao mongoCoreDao;
    private final MongoSourceDao mongoSourceDao;
    private final RedissonClient redissonClient;
    private final ApplicationProperties applicationProperties;
    private final IndexerPool indexerPool;
    private final RecordsProcessor recordsProcessor;

    private static final Map<Class<?>, String> retryExceptions;

    static {
        retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
        retryExceptions.put(MongoWriteException.class, "E11000 duplicate key error collection");
    }


    public ProcessorRunner(ApplicationProperties applicationProperties, MongoProcessorDao mongoProcessorDao, MongoCoreDao mongoCoreDao, MongoSourceDao mongoSourceDao, RedissonClient redissonClient, IndexerPool indexerPool) {
        this.applicationProperties = applicationProperties;
        this.mongoProcessorDao = mongoProcessorDao;
        this.mongoCoreDao = mongoCoreDao;
        this.mongoSourceDao = mongoSourceDao;
        this.redissonClient = redissonClient;
        this.indexerPool = indexerPool;
        this.recordsProcessor = new RecordsProcessor(applicationProperties.getRecordParallelThreads());
    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("START");

        initializeLockWrapped();
        DatasetPage datasetPage = getNextPageLockWrapped();
        while (!datasetPage.getFullBeanList().isEmpty()) {
            LOGGER.info("Processing dataset {} - page {}", datasetPage.getDatasetId(), datasetPage.getPage());
            pageProcess(datasetPage);
            updateDatasetStatusLockWrapped(datasetPage);
            datasetPage = getNextPageLockWrapped();
        }

        recordsProcessor.close();
        LOGGER.info("END");
    }

    private void pageProcess(DatasetPage datasetPage) throws InterruptedException {
        try {
            List<RDF> rdfs = recordsProcessor.process(datasetPage.getFullBeanList());
            // TODO: 26/07/2023 Handle error pages?
        } catch (ExecutionException e) {
            LOGGER.error("{} - Could not process page: {}", datasetPage.getDatasetId(), datasetPage.getPage(), e);
            exceptionStacktraceToString(e);
        } catch (RuntimeException e) {
            LOGGER.error("{} - Could not process or index(RuntimeException) page: {}", datasetPage.getDatasetId(), datasetPage.getPage(), e);
            exceptionStacktraceToString(e);
        }
    }

    // TODO: 26/07/2023 With indexing example
//    private void pageProcess(DatasetPage datasetPage) throws InterruptedException {
//        try {
//            List<RDF> rdfs = recordsProcessor.process(datasetPage.getFullBeanList());
//            indexRdfs(rdfs);
//            // TODO: 26/07/2023 Handle error pages?
//        } catch (ExecutionException e) {
//            LOGGER.error("{} - Could not process page: {}", datasetPage.getDatasetId(), datasetPage.getPage(), e);
//            exceptionStacktraceToString(e);
//        } catch (IndexingException e) {
//            LOGGER.error("{} - Could not index page: {}", datasetPage.getDatasetId(), datasetPage.getPage(), e);
//            exceptionStacktraceToString(e);
//        } catch (RuntimeException e) {
//            LOGGER.error("{} - Could not process or index(RuntimeException) page: {}", datasetPage.getDatasetId(), datasetPage.getPage(), e);
//            exceptionStacktraceToString(e);
//        }
//    }

    private static String exceptionStacktraceToString(Exception e) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        ps.close();
        return baos.toString();
    }

    private void indexRdfs(List<RDF> rdfs) throws RecordRelatedIndexingException {
        //Timestamps should be preserved, Redirects calculation disabled
        final Date recordDate = null;
        final List<String> datasetIdsForRedirection = null;
        final boolean performRedirects = false;
        final boolean tierRecalculation = false;
        final boolean preserveTimestamps = true;
        final Set<EdmType> typesEnabledForTierCalculation = EnumSet.of(EdmType._3_D);
        final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
                datasetIdsForRedirection, performRedirects, tierRecalculation, typesEnabledForTierCalculation);

        for (RDF rdf : rdfs) {
            try {
                ExternalRequestUtil.retryableExternalRequest(() -> {
                    indexerPool.indexRdf(rdf, indexingProperties);
                    return null;
                }, retryExceptions);
            } catch (Exception e) {
                throw new RecordRelatedIndexingException("A Runtime Exception occurred", e);
            }
        }
    }

    private void updateDatasetStatusLockWrapped(DatasetPage datasetPage) {
        if (datasetPage.getDatasetId() != null) {
            //Distributed LOCK for datasetStatuses update
            RLock lock = redissonClient.getFairLock(DATASET_STATUS);
            lock.lock();
            try {
                DatasetStatus datasetStatus = mongoProcessorDao.getDatasetStatus(datasetPage.getDatasetId());
                final Set<Integer> pagesProcessed = datasetStatus.getPagesProcessed();
                pagesProcessed.add(datasetPage.getPage());
                datasetStatus.getCurrentPagesProcessing().remove(datasetPage.getPage());
                mongoProcessorDao.storeDatasetStatusToDb(datasetStatus);
            } finally {
                lock.unlock();
            }
            //Distributed UNLOCK for datasetStatuses update
        }
    }

    private void initializeLockWrapped() {
        //Distributed LOCK for datasetStatuses initialization
        RLock lock = redissonClient.getFairLock(DATASET_STATUS);
        lock.lock();
        try {
            //Request configuration Mongo database for datasetStatuses
            List<DatasetStatus> allDatasetStatuses = mongoProcessorDao.getAllDatasetStatuses();

            //If datasetStatuses is already initialized, do nothing
            //If datasetStatuses is NOT already initialized, initialize
            if (allDatasetStatuses.isEmpty()) {
                LOGGER.info("DatasetStatuses: is empty.");
                LOGGER.info("DatasetStatuses: Start initialization.");
//                Map<String, Long> datasetsWithSize = getDatasetWithSize(10);
                Map<String, Long> datasetsWithSize = getDatasetWithSize();
                //--Remove this block
                Optional<Map.Entry<String, Long>> entry = datasetsWithSize.entrySet().stream().findFirst();
                entry.ifPresent(stringLongEntry -> LOGGER.info("{}: {}", stringLongEntry.getKey(), stringLongEntry.getValue()));
                //--Remove this block
                allDatasetStatuses = orderAndInitialDatasetStatuses(datasetsWithSize);
                LOGGER.info("DatasetStatuses: End initialization.");
            }

            //--Remove this block
            Optional<DatasetStatus> datasetStatus = allDatasetStatuses.stream().findFirst();
            datasetStatus.ifPresent(datasetStatusItem -> LOGGER.info("{}: {}", datasetStatusItem.getDatasetId(), datasetStatusItem.getTotalRecords()));
            //--Remove this block
        }
        finally {
            lock.unlock();
        }
        //Distributed UNLOCK for datasetStatuses initialization
    }

    private List<DatasetStatus> orderAndInitialDatasetStatuses(Map<String, Long> datasetsWithSize) {
        //Order datasetId by actual size
        AtomicInteger atomicIndex = new AtomicInteger(0);
        return datasetsWithSize.entrySet().stream().filter(entry -> entry.getValue() > 0)
                .sorted(Collections.reverseOrder(comparingByValue())).map(
                        entry -> retrieveOrInitializeDatasetStatus(entry.getKey(),
                                atomicIndex.getAndIncrement(), entry.getValue())).collect(Collectors.toList());
    }

    /**
     * Either get a {@link DatasetStatus} that already exists or generate one.
     *
     * @param indexInOrderedList the index of the dataset in the original ordered list
     * @return the dataset status
     */
    private DatasetStatus retrieveOrInitializeDatasetStatus(String datasetId, int indexInOrderedList,
                                                            long totalRecordsForDataset) {
        DatasetStatus retrievedDatasetStatus = mongoProcessorDao.getDatasetStatus(datasetId);
        if (retrievedDatasetStatus == null) {
            retrievedDatasetStatus = new DatasetStatus();
            retrievedDatasetStatus.setDatasetId(datasetId);
            retrievedDatasetStatus.setIndexInOrderedList(indexInOrderedList);
            retrievedDatasetStatus.setTotalRecords(totalRecordsForDataset);
            mongoProcessorDao.storeDatasetStatusToDb(retrievedDatasetStatus);
        }
        return retrievedDatasetStatus;
    }


    private Map<String, Long> getDatasetWithSize() {
        return getDatasetWithSize(Long.MAX_VALUE);
    }

    private Map<String, Long> getDatasetWithSize(long limit) {
        //Request all datasetIds from metis core database(our source of truth)
        return mongoCoreDao.getAllDatasetIds().parallelStream().limit(limit).collect(
                toMap(Function.identity(), mongoSourceDao::getTotalRecordsForDataset));
    }

    private DatasetPage getNextPageLockWrapped() {
        //Distributed LOCK for page processing
        RLock lock = redissonClient.getFairLock(DATASET_STATUS);
        lock.lock();
        try {
            DatasetPageBuilder datasetPageNumber = mongoProcessorDao.getNextDatasetPageNumber();
            if (datasetPageNumber.getDatasetId() == null) {
                datasetPageNumber.setFullBeanList(Collections.emptyList());
            } else {
                datasetPageNumber.setFullBeanList(
                        mongoSourceDao.getNextPageOfRecords(datasetPageNumber.getDatasetId(), datasetPageNumber.getPage(), applicationProperties.getRecordPageSize()));
            }
            return datasetPageNumber.build();
        } finally {
            lock.unlock();
        }
        //Distributed UNLOCK for page processing
    }
}
