package eu.europeana.metis.processor.utilities;

import eu.europeana.corelib.definitions.edm.beans.FullBean;
import eu.europeana.corelib.definitions.edm.entity.Aggregation;
import eu.europeana.corelib.definitions.edm.entity.WebResource;
import eu.europeana.metis.exception.GenericMetisException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class FullbeanUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
//    private final MongoSourceDao mongoSourceDao;
//
//    public FullbeanUtil(MongoSourceDao mongoSourceDao) {
//        this.mongoSourceDao = mongoSourceDao;
//    }

    public FullbeanUtil() {
    }

    public void injectWebResourceMetaInfo(final FullBean fullBean) {
//        Map<String, WebResource> webResourceHashCodes = prepareWebResourceHashCodes(fullBean);
//        final List<WebResourceMetaInfoImpl> webResourceMetaInfos = mongoSourceDao
//                .getTechnicalMetadataForHashCodes(new ArrayList<>(webResourceHashCodes.keySet()));
//        for (WebResourceMetaInfoImpl webResourceMetaInfo : webResourceMetaInfos) {
//            WebResource webResource = webResourceHashCodes.get(webResourceMetaInfo.getId());
//            ((WebResourceImpl) webResource).setWebResourceMetaInfo(webResourceMetaInfo);
//        }
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

    private static void checkMatchingWebResourcesAndGenerateHash(String fullBeanAbout,
                                                                 Aggregation aggregation, Set<String> urls, Map<String, WebResource> hashCodes) {
        for (final WebResource webResource : aggregation.getWebResources()) {
            if (!urls.contains(webResource.getAbout().trim())) {
                continue;
            }

            try {
                // Locate the technical meta data from the web resource about
                if (webResource.getAbout() != null) {
                    String hashCodeAbout = md5Hex(webResource.getAbout() + "-" + fullBeanAbout);
                    hashCodes.put(hashCodeAbout, webResource);
                }

                // Locate the technical meta data from the aggregation is shown by
                if (!hashCodes.containsValue(webResource) && aggregation.getEdmIsShownBy() != null) {
                    String hashCodeIsShownBy = md5Hex(
                            aggregation.getEdmIsShownBy() + "-" + aggregation.getAbout());
                    hashCodes.put(hashCodeIsShownBy, webResource);
                }

                // Locate the technical meta data from the aggregation is shown at
                if (!hashCodes.containsValue(webResource) && aggregation.getEdmIsShownAt() != null) {
                    String hashCodeIsShownAt = md5Hex(
                            aggregation.getEdmIsShownAt() + "-" + aggregation.getAbout());
                    hashCodes.put(hashCodeIsShownAt, webResource);
                }
            } catch (GenericMetisException e) {
                LOGGER.warn("Something went wrong during hash, skipping web resource metadata {}",
                        webResource.getAbout());
            }
        }
    }

    /**
     * Converts a string to md5 hash.
     *
     * @param stringToMd5 the string to convert
     * @return the md5 hash of the string
     * @throws GenericMetisException if the md5 could not be generated
     */
    private static String md5Hex(final String stringToMd5) throws GenericMetisException {
        try {
            byte[] bytes = stringToMd5.getBytes(StandardCharsets.UTF_8.name());
            byte[] md5bytes = MessageDigest.getInstance("MD5").digest(bytes);
            return String.format("%032x", new BigInteger(1, md5bytes));
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
            throw new GenericMetisException("Could not compute md5 hash", e);
        }
    }
}
