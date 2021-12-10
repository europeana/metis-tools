package eu.europeana.metis.dataset.sizes.updater;


import eu.europeana.metis.dataset.sizes.updater.configuration.SocksProxyConfig;
import eu.europeana.metis.dataset.sizes.updater.configuration.TrustStoreConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.embedded.EmbeddedMongoAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication(exclude = {EmbeddedMongoAutoConfiguration.class})
public class DatasetSizesUpdaterMain {

  public static void main(String[] args) {
    final ConfigurableApplicationContext context = SpringApplication.run(
        new Class[]{DatasetSizesUpdaterMain.class, TrustStoreConfig.class, SocksProxyConfig.class}, args);
    final ApplicationRunner applicationRunner = context.getBean(ApplicationRunner.class);
    applicationRunner.run();
    context.close();
  }
}
