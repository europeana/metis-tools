package eu.europeana.metis.processor.utilities;

import static eu.europeana.metis.mediaprocessing.extraction.ThumbnailGenerator.md5Hex;

import eu.europeana.corelib.definitions.edm.beans.FullBean;
import eu.europeana.corelib.definitions.edm.entity.Aggregation;
import eu.europeana.corelib.definitions.edm.entity.WebResource;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullbeanUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public FullbeanUtil() {
  }

  private static void checkMatchingWebResourcesAndGenerateHash(String fullBeanAbout,
      Aggregation aggregation, Set<String> urls, Map<String, WebResource> hashCodes) {
    for (final WebResource webResource : aggregation.getWebResources()) {
      if (!urls.contains(webResource.getAbout().trim())) {
        continue;
      }

      try {
        // Locate the technical metadata from the web resource about
        if (webResource.getAbout() != null) {
          String hashCodeAbout = md5Hex(webResource.getAbout() + "-" + fullBeanAbout);
          hashCodes.put(hashCodeAbout, webResource);
        }

        // Locate the technical metadata from the aggregation is shown by
        if (!hashCodes.containsValue(webResource) && aggregation.getEdmIsShownBy() != null) {
          String hashCodeIsShownBy = md5Hex(
              aggregation.getEdmIsShownBy() + "-" + aggregation.getAbout());
          hashCodes.put(hashCodeIsShownBy, webResource);
        }

        // Locate the technical metadata from the aggregation is shown at
        if (!hashCodes.containsValue(webResource) && aggregation.getEdmIsShownAt() != null) {
          String hashCodeIsShownAt = md5Hex(
              aggregation.getEdmIsShownAt() + "-" + aggregation.getAbout());
          hashCodes.put(hashCodeIsShownAt, webResource);
        }
      } catch (MediaExtractionException e) {
        LOGGER.warn("Something went wrong during hash, skipping web resource metadata {}",
            webResource.getAbout());
      }
    }
  }

  public void injectWebResourceMetaInfo(Map<String, WebResource> webResourceHashCodes,
      List<WebResourceMetaInfoImpl> webResourceMetaInfos) {
    for (WebResourceMetaInfoImpl webResourceMetaInfo : webResourceMetaInfos) {
      WebResource webResource = webResourceHashCodes.get(webResourceMetaInfo.getId());
      ((WebResourceImpl) webResource).setWebResourceMetaInfo(webResourceMetaInfo);
    }
  }

  public Map<String, WebResource> prepareWebResourceHashCodes(FullBean fullBean) {
    Map<String, WebResource> hashCodes = new HashMap<>();

    for (final Aggregation aggregation : fullBean.getAggregations()) {
      final Set<String> urls = new HashSet<>();

      if (StringUtils.isNotEmpty(aggregation.getEdmIsShownBy())) {
        urls.add(aggregation.getEdmIsShownBy());
      }

      if (StringUtils.isNotEmpty(aggregation.getEdmIsShownAt())) {
        urls.add(aggregation.getEdmIsShownAt());
      }

      if (null != aggregation.getHasView()) {
        urls.addAll(Arrays.asList(aggregation.getHasView()));
      }

      if (null != aggregation.getEdmObject()) {
        urls.add(aggregation.getEdmObject());
      }

      checkMatchingWebResourcesAndGenerateHash(fullBean.getAbout(), aggregation, urls, hashCodes);
    }
    return hashCodes;
  }
}
