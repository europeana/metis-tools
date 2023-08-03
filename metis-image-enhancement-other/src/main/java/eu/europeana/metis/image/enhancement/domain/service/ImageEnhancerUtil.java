package eu.europeana.metis.image.enhancement.domain.service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.metis.image.enhancement.client.ImageEnhancerClient;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.extraction.CommandExecutor;
import eu.europeana.metis.mediaprocessing.extraction.ImageMetadata;
import eu.europeana.metis.mediaprocessing.extraction.ThumbnailGenerator;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.ThumbnailKind;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.WebResourceType;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Objects;
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
        extractThumbnailEnhanceAndReUploadIt(recordToProcess);
        final long elapsedTimeInNanoSec = System.nanoTime() - startTime;
        LOGGER.info("elapsed time for whole processing {}ns", elapsedTimeInNanoSec);
    }

    private void extractThumbnailEnhanceAndReUploadIt(RDF recordToProcess) {

        List<WebResourceType> thumbnailResourceList = recordToProcess.getWebResourceList()
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        thumbnailResourceList.forEach(thumbnailResource -> {
            try {
                final String smallThumbnailObjectName = ThumbnailUtil.getThumbnailName(thumbnailResource.getAbout(), ThumbnailKind.MEDIUM);
                final String largeThumbnailObjectName = ThumbnailUtil.getThumbnailName(thumbnailResource.getAbout(), ThumbnailKind.LARGE);
                if (hasThumbnailResolutionLowerThan400(thumbnailResource)) {
                    LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), largeThumbnailObjectName);
                    byte[] enhancedLargeThumbnailData = enhanceThumbnail(thumbnailResource, largeThumbnailObjectName);
                    // enhancedLargeThumbnail comes with media processed info
                    s3Client.putObject(largeThumbnailObjectName, new ByteArrayInputStream(enhancedLargeThumbnailData), prepareObjectMetadata(thumbnailResource));
                    if (hasThumbnailResolutionLowerThan200(thumbnailResource)) {
                        LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), smallThumbnailObjectName);
                        byte[] enhancedSmallThumbnailData = enhanceThumbnail(thumbnailResource, smallThumbnailObjectName);
                        // enhancedSmallThumbnail comes with media processed info
                        s3Client.putObject(smallThumbnailObjectName, new ByteArrayInputStream(enhancedSmallThumbnailData), prepareObjectMetadata(thumbnailResource));
                    }
                }
            } catch (MediaExtractionException | IOException e) {
                LOGGER.error("enhancing thumbnail image {} {}", thumbnailResource.getAbout(), e);
            }
        });
    }
    private byte[] enhanceThumbnail(WebResourceType resource, String thumbnailName) throws IOException {
        byte[] result = this.s3Client.getObject(thumbnailName);
        String fileExtension = "";
        if ((resource.getHasMimeType() != null && resource.getHasMimeType().getHasMimeType().equals("image/jpeg"))
                || resource.getAbout().substring(resource.getAbout().lastIndexOf(".") + 1).equals("jpg")
                || resource.getAbout().substring(resource.getAbout().lastIndexOf(".") + 1).equals("jpeg")
        ) {
            fileExtension = ".jpeg";

        }
        if ((resource.getHasMimeType() != null && resource.getHasMimeType().getHasMimeType().equals("image/png"))
                || resource.getAbout().substring(resource.getAbout().lastIndexOf(".") + 1).equals("png")) {
            fileExtension = ".png";
        }
        final String fileName = "/tmp/" + thumbnailName + fileExtension;
        try (FileOutputStream fos = new FileOutputStream("/tmp/" + thumbnailName + fileExtension)) {
            fos.write(result);
        } catch (IOException e) {
            LOGGER.error("writing temp file");
        }
        byte[] enhancedImage = this.imageEnhancerClient.enhance(result);
        try {
            ThumbnailGenerator thumbnailGenerator = new ThumbnailGenerator(new CommandExecutor(300));
            Pair<ImageMetadata, List<Thumbnail>> output = thumbnailGenerator.generateThumbnails(thumbnailName, resource.getHasMimeType().getHasMimeType(), new File(fileName), true);
            List<Thumbnail> resizedThumbnails = output.getRight();
            for (Thumbnail thumbnail : resizedThumbnails) {
                // final size
                enhancedImage = thumbnail.getContentStream().readAllBytes();
                try (FileOutputStream fos = new FileOutputStream("/tmp/" + thumbnail.getTargetName() + fileExtension)) {
                    fos.write(enhancedImage);
                } catch (IOException e) {
                    LOGGER.error("writing temp file");
                }
            }
        } catch (MediaProcessorException | MediaExtractionException e) {
            LOGGER.error("running extracting media", e);
        }

        return enhancedImage;
    }

    private boolean hasThumbnailResolutionLowerThan400(WebResourceType thumbnailResource) {
        return thumbnailResource.getHeight() != null && thumbnailResource.getWidth() != null &&
                Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 400;
    }

    private boolean hasThumbnailResolutionLowerThan200(WebResourceType thumbnailResource) {
        return thumbnailResource.getHeight() != null && thumbnailResource.getWidth() != null &&
                Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 200;
    }

    private ObjectMetadata prepareObjectMetadata(WebResourceType thumbnail) {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(thumbnail.getHasMimeType().getHasMimeType());
        objectMetadata.setContentLength(thumbnail.getFileByteSize().getLong());
        return objectMetadata;
    }
}
