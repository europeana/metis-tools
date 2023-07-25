package eu.europeana.metis.processor;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class RecordsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final ExecutorService threadPool;
    private final ExecutorCompletionService<Void> completionService;

    public RecordsProcessor(int maxThreads) {
        this.threadPool = Executors.newFixedThreadPool(maxThreads);
        this.completionService = new ExecutorCompletionService<>(threadPool);
    }

    public void process(List<FullBeanImpl> fullBeans) throws InterruptedException {

        List<Future<Void>> futureList = new ArrayList<>(fullBeans.size());
        for (FullBeanImpl fullbean : fullBeans) {
            RecordCallable recordCallable = new RecordCallable(fullbean);
            futureList.add(completionService.submit(recordCallable));
        }

        for (Future<Void> future : futureList){
            try {
                future.get();
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for Future", e);
                throw e;
            } catch (ExecutionException e) {
                // TODO: 24/07/2023 Handle exceptions for records;
                LOGGER.error("Exception while waiting for Future", e);
            }
        }

        LOGGER.info("Storing results.");
        for (FullBeanImpl fullbean : fullBeans) {
            //Store fullbeans in target database if necessary
            //This can also be done in the first loop instead

        }

    }

    public void close() {
        threadPool.shutdown();
        LOGGER.info("Thread pool closed.");
    }
}
