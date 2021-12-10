package eu.europeana.metis.dataset.sizes.updater.configuration;

import eu.europeana.metis.utils.CustomTruststoreAppender;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

@Configuration
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TrustStoreConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(TrustStoreConfig.class);

  @Value("${truststore.path}")
  private String truststorePath;
  @Value("${truststore.password}")
  private String truststorePassword;

  @PostConstruct
  public void loadTrustStore() throws TrustStoreConfigurationException {
    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(truststorePath) && StringUtils
        .isNotEmpty(truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(truststorePath, truststorePassword);
    }
  }

}
