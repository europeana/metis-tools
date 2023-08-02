package eu.europeana.metis.processor;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.processor.utilities.S3Client;
import eu.europeana.metis.schema.jibx.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class RecordsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final ExecutorService threadPool;
    private final S3Client s3Client;
    private final ExecutorCompletionService<RDF> completionService;

    public RecordsProcessor(int maxThreads, S3Client s3Client) {
        this.threadPool = Executors.newFixedThreadPool(maxThreads);
        this.s3Client = s3Client;
        this.completionService = new ExecutorCompletionService<>(threadPool);
    }

    public List<RDF> process(List<FullBeanImpl> fullBeans) throws InterruptedException, ExecutionException {

        List<Future<RDF>> futureList = new ArrayList<>(fullBeans.size());
        for (FullBeanImpl fullbean : fullBeans) {
            RecordCallable recordCallable = new RecordCallable(fullbean, s3Client);
            futureList.add(completionService.submit(recordCallable));
        }

        List<RDF> rdfs = new ArrayList<>(fullBeans.size());
        for (Future<RDF> future : futureList){
            try {
                rdfs.add(future.get());
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for Future", e);
                throw new InterruptedException();
            } catch (ExecutionException e) {
                LOGGER.error("Exception while waiting for Future", e);
                throw e;
            }
        }
        return rdfs;
    }

    public void close() {
        threadPool.shutdown();
        LOGGER.info("Thread pool closed.");
    }
}
