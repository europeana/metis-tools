package eu.europeana.metis.dataset.sizes.updater.configuration;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SocksProxyConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SocksProxyConfig.class);

  @Value("${socks.proxy.enabled}")
  private boolean socksProxyEnabled;
  @Value("${socks.proxy.host}")
  private String socksProxyHost;
  @Value("${socks.proxy.port}")
  private String socksProxyPort;
  @Value("${socks.proxy.username}")
  private String socksProxyUsername;
  @Value("${socks.proxy.password}")
  private String socksProxyPassword;

  @PostConstruct
  public void configureSocksProxy() {
    LOGGER.info("Configuring socks proxy");
    if (socksProxyEnabled) {
      System.setProperty("socksProxyHost", socksProxyHost);
      System.setProperty("socksProxyPort", socksProxyPort);
      Authenticator.setDefault(new Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(socksProxyUsername, socksProxyPassword.toCharArray());
        }
      });
    }
  }
}
