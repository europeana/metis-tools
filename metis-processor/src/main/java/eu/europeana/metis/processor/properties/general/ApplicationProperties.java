package eu.europeana.metis.processor.properties.general;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ApplicationProperties {

    @Value("${record.parallel.threads}")
    private int recordParallelThreads;

    @Value("${record.page.size}")
    private int recordPageSize;

    public int getRecordParallelThreads() {
        return recordParallelThreads;
    }

    public int getRecordPageSize() {
        return recordPageSize;
    }
}
