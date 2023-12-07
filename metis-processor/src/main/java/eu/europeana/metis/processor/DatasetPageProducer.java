package eu.europeana.metis.processor;

import eu.europeana.metis.processor.utilities.DatasetPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

class DatasetPageProducer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final BlockingQueue<DatasetPage> blockingQueue;
    private final Supplier<DatasetPage> supplier;

    public DatasetPageProducer(BlockingQueue<DatasetPage> blockingQueue, Supplier<DatasetPage> supplier) {
        this.blockingQueue = blockingQueue;
        this.supplier = supplier;
    }

    public void run() {
        try {
            DatasetPage datasetPage;
            do {
                datasetPage = supplier.get();
                blockingQueue.put(datasetPage);
            }while(!datasetPage.getFullBeanList().isEmpty());
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for blocking queue", e);
            Thread.currentThread().interrupt();
        }
    }
}
