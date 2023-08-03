package eu.europeana.metis.processor.utilities;

import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.ThumbnailKind;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// TODO: 03/08/2023 Make the identical code in metis media service reusable, instead of copying this.
/**
 * The type Thumbnail util.
 */
public class ThumbnailUtil {

    private ThumbnailUtil() {
        // utility class for thumbnail
    }
    /**
     * Gets thumbnail name.
     *
     * @param url  the url
     * @param kind the kind
     * @return the thumbnail name
     * @throws MediaExtractionException the media extraction exception
     */
    public static String getThumbnailName(String url, ThumbnailKind kind) throws MediaExtractionException {
        return md5Hex(url) + kind.getNameSuffix();
    }

    private static String md5Hex(String s) throws MediaExtractionException {
        try {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            // Note: we have no choice but to use MD5, this is agreed upon with the API implementation.
            // The data used are not private and are considered safe
            @SuppressWarnings({"findsecbugs:WEAK_MESSAGE_DIGEST_MD5", "java:S4790"})
            byte[] md5bytes = MessageDigest.getInstance("MD5").digest(bytes);
            return String.format("%032x", new BigInteger(1, md5bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new MediaExtractionException("Could not compute md5 hash", e);
        }
    }
}
