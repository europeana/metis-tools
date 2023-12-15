package eu.europeana.metis.processor.utilities;

import eu.europeana.corelib.definitions.model.RightsOption;
import eu.europeana.indexing.utils.RdfWrapper;
import eu.europeana.metis.schema.jibx.RDF;
import java.util.Set;

public class RdfUtil {

  public RdfUtil() {
  }

  public boolean hasThumbnailsAndValidLicense(RDF rdfRecord) {
    RdfWrapper rdfWrapper = new RdfWrapper(rdfRecord);
    boolean validLicense = rdfRecord.getAggregationList().stream().allMatch(a -> isValidLicense(a.getRights().getResource()));
    boolean hasThumbnails = rdfWrapper.hasThumbnails();
    return hasThumbnails && validLicense;
  }

  private boolean isValidLicense(String rights) {
    Set<String> validLicenses = Set.of(
        RightsOption.CC_BY.getUrl(),
        RightsOption.CC_ZERO.getUrl(),
        RightsOption.CC_BY_SA.getUrl(),
        RightsOption.CC_NOC.getUrl(),
        RightsOption.CC_BY_NC_SA.getUrl(),
        RightsOption.CC_BY_NC_ND.getUrl(),
        RightsOption.CC_BY_ND.getUrl(),
        RightsOption.CC_BY_NC.getUrl()
    );

    for (String validLicense : validLicenses) {
        if (rights != null && rights.startsWith(validLicense)) {
            return true;
        }
    }
    return false;
  }
}
