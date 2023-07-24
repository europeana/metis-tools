package eu.europeana.metis.processor;

import eu.europeana.metis.processor.dao.DatasetStatus;
import eu.europeana.metis.processor.dao.MongoCoreDao;
import eu.europeana.metis.processor.dao.MongoProcessorDao;
import eu.europeana.metis.processor.dao.MongoSourceDao;
import eu.europeana.metis.processor.utilities.DatasetPage;
import eu.europeana.metis.processor.utilities.DatasetPage.DatasetPageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

// TODO: 24/07/2023 Implement reprocessing of failed pages
// TODO: 24/07/2023 Amount of threads per app should be configurable
// TODO: 24/07/2023 Implement locks
public class ProcessorRunner implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final MongoProcessorDao mongoProcessorDao;
    private final MongoCoreDao mongoCoreDao;
    private final MongoSourceDao mongoSourceDao;
    private final RecordsProcessor recordsProcessor;

    public ProcessorRunner(MongoProcessorDao mongoProcessorDao, MongoCoreDao mongoCoreDao, MongoSourceDao mongoSourceDao) {
        this.mongoProcessorDao = mongoProcessorDao;
        this.mongoCoreDao = mongoCoreDao;
        this.mongoSourceDao = mongoSourceDao;
        // TODO: 24/07/2023 Fix this parameter
        recordsProcessor = new RecordsProcessor(1);
    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("START");

        initializeLockWrapped();
        DatasetPage datasetPage = getNextPageLockWrapped();
        while (!datasetPage.getFullBeanList().isEmpty()) {
            LOGGER.info("Processing page {}", datasetPage.getPage());
            recordsProcessor.process(datasetPage.getFullBeanList());
            updateDatasetStatusLockWrapped(datasetPage);
            datasetPage = getNextPageLockWrapped();
        }

        recordsProcessor.close();
        LOGGER.info("END");
    }

    private void updateDatasetStatusLockWrapped(DatasetPage datasetPage) {
        if (datasetPage.getDatasetId() != null) {
            //Distributed LOCK for datasetStatuses update
            DatasetStatus datasetStatus = mongoProcessorDao.getDatasetStatus(datasetPage.getDatasetId());
            final Set<Integer> pagesProcessed = datasetStatus.getPagesProcessed();
            pagesProcessed.add(datasetPage.getPage());
            mongoProcessorDao.storeDatasetStatusToDb(datasetStatus);
            //Distributed UNLOCK for datasetStatuses update
        }
    }

    private void initializeLockWrapped() {
        //Distributed LOCK for datasetStatuses initialization
        //Request configuration Mongo database for datasetStatuses
        List<DatasetStatus> allDatasetStatuses = mongoProcessorDao.getAllDatasetStatuses();

        //If datasetStatuses is already initialized, do nothing
        //If datasetStatuses is NOT already initialized, initialize
        if (allDatasetStatuses.isEmpty()) {
            Map<String, Long> datasetsWithSize = getDatasetWithSize(10);
            Optional<Map.Entry<String, Long>> entry = datasetsWithSize.entrySet().stream().findFirst();
            entry.ifPresent(stringLongEntry -> LOGGER.info("{}: {}", stringLongEntry.getKey(), stringLongEntry.getValue()));
            allDatasetStatuses = orderAndInitialDatasetStatuses(datasetsWithSize);
        }
        //Distributed UNLOCK for datasetStatuses initialization

        //--Remove this part
        Optional<DatasetStatus> datasetStatus = allDatasetStatuses.stream().findFirst();
        datasetStatus.ifPresent(datasetStatusItem -> LOGGER.info("{}: {}", datasetStatusItem.getDatasetId(), datasetStatusItem.getTotalRecords()));
        //--Remove this part
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
        // TODO: 24/07/2023 Get actual size per datasetId based on mongo query(can be any type of query, with the simplest being filtering by datasetId only)
        return mongoCoreDao.getAllDatasetIds().parallelStream().limit(limit).collect(
                toMap(Function.identity(), mongoSourceDao::getTotalRecordsForDataset));
    }

    private DatasetPage getNextPageLockWrapped() {
        //Distributed LOCK for page processing
        DatasetPageBuilder datasetPageNumber = mongoProcessorDao.getNextDatasetPageNumber();
        if (datasetPageNumber.getDatasetId() == null) {
            datasetPageNumber.setFullBeanList(Collections.emptyList());
        } else {
            datasetPageNumber.setFullBeanList(mongoSourceDao.getNextPageOfRecords(datasetPageNumber.getDatasetId(), datasetPageNumber.getPage()));
        }
        //Distributed UNLOCK for page processing
        return datasetPageNumber.build();
    }
}
