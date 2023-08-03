package eu.europeana.metis.processor.utilities;

import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.metis.image.enhancement.client.ImageEnhancerClient;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.ThumbnailKind;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.WebResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Enhancement processor.
 */
public class ImageEnhancerUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final S3Client s3Client;
    private final ImageEnhancerClient imageEnhancerClient;

    /**
     * Instantiates a new Enhancement processor.
     *
     * @param s3Client            the s3 client
     * @param imageEnhancerClient the image enhancer client
     */
    public ImageEnhancerUtil(S3Client s3Client, ImageEnhancerClient imageEnhancerClient) {
        this.s3Client = s3Client;
        this.imageEnhancerClient = imageEnhancerClient;
    }


    /**
     * Process record.
     *
     * @param recordToProcess the record to process
     */
    public void processRecord(RDF recordToProcess) {
        final long startTime = System.nanoTime();
        extractThumbnailEnhanceAndReuploadIt(recordToProcess);
        final long elapsedTimeInNanoSec = System.nanoTime() - startTime;
        LOGGER.info("elapsed time for whole processing {}ns", elapsedTimeInNanoSec);
    }

    private void extractThumbnailEnhanceAndReuploadIt(RDF recordToProcess) {

        List<WebResourceType> thumbnailResourceList = recordToProcess.getWebResourceList()
                .stream()
                .filter(thumbnailResource -> thumbnailResource.getHasMimeType().getHasMimeType().equals("image/jpeg")
                        || thumbnailResource.getHasMimeType().getHasMimeType().equals("image/png"))
                .collect(Collectors.toList());

        thumbnailResourceList.forEach(thumbnailResource -> {
            try {
                final String smallThumbnailObjectName = ThumbnailUtil.getThumbnailName(thumbnailResource.getAbout(), ThumbnailKind.MEDIUM);
                final String largeThumbnailObjectName = ThumbnailUtil.getThumbnailName(thumbnailResource.getAbout(), ThumbnailKind.LARGE);
                if (hasThumbnailResolutionLowerThan400(thumbnailResource)) {
                    LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), largeThumbnailObjectName);
                    byte[] largeThumbnail = s3Client.getObject(largeThumbnailObjectName);
                    byte[] enhancedLargeThumbnailData = this.imageEnhancerClient.enhance(largeThumbnail);
                    // do media processing here
//                    s3Client.putObject(largeThumbnailObjectName, new ByteArrayInputStream(enhancedLargeThumbnailData), prepareObjectMetadata(thumbnailResource));
                    if (hasThumbnailResolutionLowerThan200(thumbnailResource)) {
                        LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), smallThumbnailObjectName);
                        // do media processing here
                        //s3Client.putObject(smallThumbnailObjectName, new ByteArrayInputStream(enhancedSmallThumbnailData), prepareObjectMetadata(thumbnailResource));
                    }
                }
            } catch (MediaExtractionException e) {
                LOGGER.error("enhancing thumbnail image {} {}", thumbnailResource.getAbout(), e);
            }
        });
    }
    private boolean hasThumbnailResolutionLowerThan400(WebResourceType thumbnailResource) {
        return Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 400;
    }

    private boolean hasThumbnailResolutionLowerThan200(WebResourceType thumbnailResource) {
        return Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 200;
    }

    private ObjectMetadata prepareObjectMetadata(WebResourceType thumbnail) {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(thumbnail.getHasMimeType().getHasMimeType());
        objectMetadata.setContentLength(thumbnail.getFileByteSize().getLong());
        return objectMetadata;
    }
}
