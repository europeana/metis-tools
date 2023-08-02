package eu.europeana.metis.processor.image.enhancer;

import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.metis.image.enhancement.client.ImageEnhancerClient;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.processor.utilities.S3Client;
import eu.europeana.metis.processor.utilities.ThumbnailUtil;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.WebResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The type Enhancement processor.
 */
public class EnhancementProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final S3Client s3Client;
    private final ImageEnhancerClient imageEnhancerClient;

    /**
     * Instantiates a new Enhancement processor.
     *
     * @param s3Client            the s3 client
     * @param imageEnhancerClient the image enhancer client
     */
    public EnhancementProcessor(S3Client s3Client, ImageEnhancerClient imageEnhancerClient) {
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
                LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), largeThumbnailObjectName);
                LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), smallThumbnailObjectName);
                if (hasThumbnailLowerResolutionThan400(thumbnailResource)) {
                    byte[] enhancedLargeThumbnailData = enhanceThumbnail(thumbnailResource, largeThumbnailObjectName);
                    if (hasThumbnailLowerResolutionThan200(thumbnailResource)) {
                        byte[] enhancedSmallThumbnailData = enhanceThumbnail(thumbnailResource, smallThumbnailObjectName);
                        // do media processing here
                        //s3Client.putObject(smallThumbnailObjectName, new ByteArrayInputStream(enhancedSmallThumbnailData), prepareObjectMetadata(thumbnailResource));
                    }
                    // do media processing here
                    //s3Client.putObject(largeThumbnailObjectName, new ByteArrayInputStream(enhancedLargeThumbnailData), prepareObjectMetadata(thumbnailResource));
                }
            } catch (MediaExtractionException | IOException e) {
                LOGGER.error("enhancing thumbnail image {} {}", thumbnailResource.getAbout(), e);
            }
        });
    }

    private byte[] enhanceThumbnail(WebResourceType resource, String thumbnailName) throws IOException {
        byte[] s3Object = s3Client.getObject(thumbnailName);
        String fileExtension = "";
        if (resource.getHasMimeType().getHasMimeType().equals("image/jpeg")) {
            fileExtension = ".jpeg";
        }
        if (resource.getHasMimeType().getHasMimeType().equals("image/png")) {
            fileExtension = ".png";
        }
        String fileName = "/tmp/" + thumbnailName + fileExtension;
        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            fos.write(s3Object);
            LOGGER.info("saving preview before {}", fileName);
        } catch (IOException e) {
            LOGGER.error("saving preview before {} {}", fileName, e);
        }
        s3Object = this.imageEnhancerClient.enhance(s3Object);

        fileName = "/tmp/" + thumbnailName + "_enhanced" + fileExtension;
        try (FileOutputStream fos = new FileOutputStream("/tmp/" + thumbnailName + "_enhanced" + fileExtension)) {
            fos.write(s3Object);
            LOGGER.info("saving preview after {}", fileName);
        } catch (IOException e) {
            LOGGER.error("saving preview after {} {}", fileName, e);
        }
        return s3Object;
    }

    private boolean hasThumbnailLowerResolutionThan400(WebResourceType thumbnailResource) {
        return Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 400;
    }

    private boolean hasThumbnailLowerResolutionThan200(WebResourceType thumbnailResource) {
        return Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 200;
    }

    private ObjectMetadata prepareObjectMetadata(WebResourceType thumbnail) {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(thumbnail.getHasMimeType().getHasMimeType());
        objectMetadata.setContentLength(thumbnail.getFileByteSize().getLong());
        return objectMetadata;
    }
}
