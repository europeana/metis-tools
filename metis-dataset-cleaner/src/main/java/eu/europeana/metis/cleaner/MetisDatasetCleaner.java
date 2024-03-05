package eu.europeana.metis.cleaner;

import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.metis.cleaner.common.TargetIndexingDatabase;
import java.util.Arrays;
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
  public void run( ApplicationArguments args ) throws Exception {
    LOGGER.info("Starting cleaning database script");
    ApplicationInitializer applicationInitializer = new ApplicationInitializer();
    // Help
    LOGGER.info("Usage: where # is dataset id number.");
    LOGGER.info("mvn spring-boot:run -Dspring-boot.run.arguments=\"--dataset.id=#\"");
    // Reading Command-Line arguments
    LOGGER.info("Application started with command-line arguments: {}", Arrays.toString(args.getSourceArgs()));
    LOGGER.info("NonOptionArgs: {}", args.getNonOptionArgs());
    LOGGER.info("OptionNames: {}", args.getOptionNames());

    for (String name : args.getOptionNames()){
      LOGGER.info("arg-" + name + "=" + args.getOptionValues(name));
    }

    boolean containsOption = args.containsOption("dataset.id");
    LOGGER.info("Contains dataset.id: " + containsOption);

    if (containsOption) {
      final String datasetId = args.getOptionValues("dataset.id").getFirst();
      LOGGER.info("cleaning preview datasetid: {}", datasetId);
      applicationInitializer.getIndexWrapper().getIndexer(TargetIndexingDatabase.PREVIEW).remove(datasetId);
      LOGGER.info("cleaning publish datasetid: {}", datasetId);
      applicationInitializer.getIndexWrapper().getIndexer(TargetIndexingDatabase.PUBLISH).remove(datasetId);
    }
    LOGGER.info("Finished cleaning database script");
    System.exit(0);
  }
}
