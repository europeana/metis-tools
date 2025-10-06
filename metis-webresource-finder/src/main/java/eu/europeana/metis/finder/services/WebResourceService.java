package eu.europeana.metis.finder.services;

import eu.europeana.metis.finder.domain.model.WebResource;
import jakarta.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WebResourceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebResourceService.class);

  public WebResource generateHashCode(String webResourceId, String recordId) {
    String generatedHash = null;
    try {
      // Note: we have no choice but to use MD5, this is agreed upon with the API implementation.
      // The data used are not private and are considered safe
      @SuppressWarnings({"findsecbugs:WEAK_MESSAGE_DIGEST_MD5", "java:S4790"})
      final MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] digest = md.digest((webResourceId + "-" + recordId).getBytes(StandardCharsets.UTF_8));
      generatedHash = DatatypeConverter.printHexBinary(digest).toLowerCase(Locale.US);
    } catch (NoSuchAlgorithmException e) {
      //This shouldn't happen
      LOGGER.error("Hashing algorithm does not exist", e);
    }
    return new WebResource(webResourceId, recordId, generatedHash);
  }
}
