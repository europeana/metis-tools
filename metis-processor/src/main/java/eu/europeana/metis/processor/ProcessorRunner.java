package eu.europeana.metis.processor;

import eu.europeana.metis.processor.dao.DatasetStatus;
import eu.europeana.metis.processor.dao.MongoCoreDao;
import eu.europeana.metis.processor.dao.MongoProcessorDao;
import eu.europeana.metis.processor.dao.MongoSourceDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

public class ProcessorRunner implements CommandLineRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorRunner.class);


    private final MongoProcessorDao mongoProcessorDao;
    private final MongoCoreDao mongoCoreDao;
    private final MongoSourceDao mongoSourceDao;

    public ProcessorRunner(MongoProcessorDao mongoProcessorDao, MongoCoreDao mongoCoreDao, MongoSourceDao mongoSourceDao) {
        this.mongoProcessorDao = mongoProcessorDao;
        this.mongoCoreDao = mongoCoreDao;
        this.mongoSourceDao = mongoSourceDao;
    }

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("START");

        //Distributed LOCK for datasetStatuses initialization
        initialize();
        //Distributed UNLOCK for datasetStatuses initialization

        //Distributed LOCK for page processing
        getNextPageAvailable();
        //Distributed UNLOCK for page processing

        LOGGER.info("END");
    }

    private void initialize() {
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
        Optional<DatasetStatus> datasetStatus = allDatasetStatuses.stream().findFirst();
        datasetStatus.ifPresent(datasetStatusItem -> LOGGER.info("{}: {}", datasetStatusItem.getDatasetId(), datasetStatusItem.getTotalRecords()));

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
        //Get actual size per datasetId based on mongo query(can be any type of query, with the simplest being filtering by datasetId only)
        return mongoCoreDao.getAllDatasetIds().parallelStream().limit(limit).collect(
                toMap(Function.identity(), mongoSourceDao::getTotalRecordsForDataset));
    }

    private int getNextPageAvailable() {
        //Request configuration Mongo database for next available page of qualified dataset
        //Check how many pages have been processed by Math.ceil((float)processedRecords/pageSize)

        //fix this
        return 0;
    }
}
