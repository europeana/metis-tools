package eu.europeana.metis.cleaner;

import eu.europeana.metis.cleaner.common.TargetIndexingDatabase;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MetisDatasetCleaner implements ApplicationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetisDatasetCleaner.class);

  public static void main(String[] args) {
    SpringApplication.run(MetisDatasetCleaner.class, args);
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    LOGGER.info("Starting cleaning database script");
    ApplicationInitializer applicationInitializer = new ApplicationInitializer();
    // Usage help
    LOGGER.info("Usage: where # is record or dataset id number.");
    LOGGER.info("mvn spring-boot:run -Dspring-boot.run.arguments=\"--record.id=#\" or \"--dataset.id=#\"");
    LOGGER.info("java -jar metis-dataset-cleaner-1.0-SNAPSHOT.jar --record.id=# or --dataset.id=#");

    // Reading command-Line arguments
    LOGGER.info("Application started with command-line arguments: {}", Arrays.toString(args.getSourceArgs()));
    LOGGER.info("NonOptionArgs: {}", args.getNonOptionArgs());
    LOGGER.info("OptionNames: {}", args.getOptionNames());

    for (String name : args.getOptionNames()) {
      LOGGER.info("arg-{}={}",name, args.getOptionValues(name));
    }

    final boolean containsRecordId = args.containsOption("record.id");
    if (containsRecordId) {
      LOGGER.info("::Contains record.id::");
      final String recordId = args.getOptionValues("record.id").getFirst();
      LOGGER.info("cleaning preview record.id: {}", recordId);
      applicationInitializer.getIndexWrapper().getIndexer(TargetIndexingDatabase.PREVIEW).remove(recordId);
      LOGGER.info("cleaning publish record.id: {}", recordId);
      applicationInitializer.getIndexWrapper().getIndexer(TargetIndexingDatabase.PUBLISH).remove(recordId);
    }

    final boolean containsDatasetId = args.containsOption("dataset.id");
    if (containsDatasetId) {
      LOGGER.info("::Contains dataset.id::");
      final String datasetId = args.getOptionValues("dataset.id").getFirst();
      LOGGER.info("cleaning preview dataset.id: {}", datasetId);
      applicationInitializer.getIndexWrapper().getIndexer(TargetIndexingDatabase.PREVIEW).removeAll(datasetId, Date.from(Instant.now()));
      LOGGER.info("cleaning publish dataset.id: {}", datasetId);
      applicationInitializer.getIndexWrapper().getIndexer(TargetIndexingDatabase.PUBLISH).removeAll(datasetId, Date.from(Instant.now()));
    }
    LOGGER.info("Finished cleaning database script");
    System.exit(0);
  }
}
