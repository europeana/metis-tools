package eu.europeana.metis.processor.properties.general;

import eu.europeana.metis.processor.config.Mode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ApplicationProperties {

    @Value("${record.parallel.threads}")
    private int recordParallelThreads;

    @Value("${record.page.size}")
    private int recordPageSize;

    @Value("${execution.mode}")
    private String mode;

    public int getRecordParallelThreads() {
        return recordParallelThreads;
    }

    public int getRecordPageSize() {
        return recordPageSize;
    }

    public Mode getMode() {
        return Mode.getModeFromEnumName(mode);
    }
}
