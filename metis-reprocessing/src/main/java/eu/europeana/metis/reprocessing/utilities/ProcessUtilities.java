package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.corelib.definitions.edm.beans.FullBean;
import eu.europeana.corelib.definitions.edm.entity.Aggregation;
import eu.europeana.corelib.definitions.edm.entity.WebResource;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.schema.jibx.RDF;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains functionality for processing a record.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the processing of records</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-15
 */
public class ProcessUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessUtilities.class);

  private ProcessUtilities() {
  }

  public static RDF processFullBean(FullBeanImpl fullBean, Configuration configuration) {
    if (configuration.isIdentityProcess()) {
      return identityProcess(fullBean, configuration.getMongoSourceMongoDao());
    } else {
      return process(fullBean, configuration);
    }
  }

  private static RDF identityProcess(FullBeanImpl fullBean,
      MongoSourceMongoDao mongoSourceMongoDao) {
    injectWebResourceMetaInfo(fullBean, mongoSourceMongoDao);
    return EdmUtils.toRDF(fullBean, true);
  }

  private static RDF process(FullBeanImpl fullBean, Configuration configuration) {
    RDF rdf = identityProcess(fullBean, configuration.getMongoSourceMongoDao());
    rdf = compute(configuration, rdf);
    return rdf;
  }

  private static RDF compute(Configuration configuration, RDF rdf) {
    return configuration.processRDF(rdf);
  }

  /**
   * Inject web resource meta info.
   *
   * @param fullBean the full bean
   * @param mongoSourceMongoDao the mongo source mongo dao
   */
  private static void injectWebResourceMetaInfo(final FullBean fullBean,
      final MongoSourceMongoDao mongoSourceMongoDao) {
    Map<String, WebResource> webResourceHashCodes = prepareWebResourceHashCodes(fullBean);
    final List<WebResourceMetaInfoImpl> webResourceMetaInfos = mongoSourceMongoDao
        .getTechnicalMetadataForHashCodes(new ArrayList<>(webResourceHashCodes.keySet()));
    for (WebResourceMetaInfoImpl webResourceMetaInfo : webResourceMetaInfos) {
      WebResource webResource = webResourceHashCodes.get(webResourceMetaInfo.getId());
      ((WebResourceImpl) webResource).setWebResourceMetaInfo(webResourceMetaInfo);
    }
  }

  /**
   * Prepare web resource hash codes map.
   *
   * @param fullBean the full bean
   * @return the map
   */
  private static Map<String, WebResource> prepareWebResourceHashCodes(FullBean fullBean) {
    Map<String, WebResource> hashCodes = new HashMap<>();
    for (final Aggregation aggregation : fullBean.getAggregations()) {
      checkMatchingWebResourcesAndGenerateHash(fullBean.getAbout(), aggregation, hashCodes);
    }
    return hashCodes;
  }

  /**
   * Check matching web resources and generate hash.
   *
   * @param fullBeanAbout the full bean about
   * @param aggregation the aggregation
   * @param hashCodes the hash codes
   */
  private static void checkMatchingWebResourcesAndGenerateHash(String fullBeanAbout,
      Aggregation aggregation, Map<String, WebResource> hashCodes) {
    for (final WebResource webResource : aggregation.getWebResources()) {
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

  /**
   * Inject web resource meta info.
   *
   * @param webResourceHashCodes the web resource hash codes
   * @param webResourceMetaInfos the web resource meta infos
   */
  public void injectWebResourceMetaInfo(Map<String, WebResource> webResourceHashCodes,
      List<WebResourceMetaInfoImpl> webResourceMetaInfos) {
    for (WebResourceMetaInfoImpl webResourceMetaInfo : webResourceMetaInfos) {
      WebResource webResource = webResourceHashCodes.get(webResourceMetaInfo.getId());
      ((WebResourceImpl) webResource).setWebResourceMetaInfo(webResourceMetaInfo);
    }
  }

  /**
   * Converts a string to md5 hash.
   *
   * @param stringToMd5 the string to convert
   * @return the md5 hash of the string
   * @throws MediaExtractionException if the md5 could not be generated
   */
  private static String md5Hex(final String stringToMd5) throws MediaExtractionException {
    try {
      byte[] bytes = stringToMd5.getBytes(StandardCharsets.UTF_8);
      byte[] md5bytes = MessageDigest.getInstance("MD5").digest(bytes);
      return String.format("%032x", new BigInteger(1, md5bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new MediaExtractionException("Could not compute md5 hash", e);
    }
  }
}
