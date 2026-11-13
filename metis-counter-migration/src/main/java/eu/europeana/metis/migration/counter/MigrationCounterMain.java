package eu.europeana.metis.migration.counter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class MigrationCounterMain {

  static void main(String[] args) {
    log.info("Starting script");
    System.exit(SpringApplication.exit(SpringApplication.run(MigrationCounterMain.class, args)));
    log.info("End script");
  }
}
