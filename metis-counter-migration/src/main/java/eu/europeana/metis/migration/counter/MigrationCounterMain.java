package eu.europeana.metis.migration.counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MigrationCounterMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(MigrationCounterMain.class);

  static void main(String[] args) {
    LOGGER.info("Starting script");
    System.exit(SpringApplication.exit(SpringApplication.run(MigrationCounterMain.class, args)));
    LOGGER.info("End script");
  }
}
