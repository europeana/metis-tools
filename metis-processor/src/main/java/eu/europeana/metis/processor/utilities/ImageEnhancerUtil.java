package eu.europeana.metis.processor.utilities;

import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.metis.image.enhancement.domain.worker.ImageEnhancerWorker;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.extraction.CommandExecutor;
import eu.europeana.metis.mediaprocessing.extraction.ImageMetadata;
import eu.europeana.metis.mediaprocessing.extraction.ThumbnailGenerator;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.ThumbnailKind;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.schema.jibx.WebResourceType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static eu.europeana.metis.mediaprocessing.extraction.ThumbnailGenerator.md5Hex;
import static eu.europeana.metis.network.ExternalRequestUtil.retryableExternalRequest;

/**
 * The type Enhancement processor.
 */
public class ImageEnhancerUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final S3Client s3Client;
    private final ImageEnhancerWorker imageEnhancerWorker;
    private final FileCsvImageReporter fileCsvImageReporter;

    private static final Map<Class<?>, String> retryExceptions;

    static {
        retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
        retryExceptions.put(NoHttpResponseException.class, "");
    }

    /**
     * Instantiates a new Enhancement processor.
     *
     * @param s3Client            the s3 client
     * @param imageEnhancerWorker the image enhancer client
     */
    public ImageEnhancerUtil(S3Client s3Client, ImageEnhancerWorker imageEnhancerWorker, FileCsvImageReporter fileCsvImageReporter) {
        this.s3Client = s3Client;
        this.imageEnhancerWorker = imageEnhancerWorker;
        this.fileCsvImageReporter = fileCsvImageReporter;
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
            ReportRow reportRow = new ReportRow();
            reportRow.setRecordId(thumbnailResource.getAbout());
            try {
                if (hasThumbnailResolutionLowerThan400(thumbnailResource)) {
                    final String largeThumbnailObjectName = md5Hex(thumbnailResource.getAbout()) + ThumbnailKind.LARGE.getNameSuffix();
                    LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), largeThumbnailObjectName);
                    byte[] largeThumbnail = s3Client.getObject(largeThumbnailObjectName);

                    final long startTime = System.nanoTime();
                    byte[] enhancedImage = enhanceImage(largeThumbnail, largeThumbnailObjectName);
                    final long elapsedTimeInNanoSec = System.nanoTime() - startTime;

                    List<Thumbnail> thumbnails = generateThumbnails(thumbnailResource, largeThumbnailObjectName, enhancedImage);
                    uploadThumbnails(thumbnailResource, thumbnails);
                    appendSuccessReport(thumbnailResource, thumbnails, elapsedTimeInNanoSec, reportRow);
                }
            } catch (MediaExtractionException | IOException e) {
                LOGGER.error("Failed enhancing thumbnail image {} {}", thumbnailResource.getAbout(), e);
                appendFailReport(reportRow);
            }
        });
    }

    private byte[] enhanceImage(byte[] largeThumbnail, String largeThumbnailObjectName) {
        return retryableExternalRequest(() -> {
            byte[] enhance;
            try {
                enhance = imageEnhancerWorker.enhance(largeThumbnail);
            } catch (Exception e) {
                LOGGER.info(largeThumbnailObjectName);
                throw e;
            }
            return enhance;

        }, retryExceptions);
    }

    private void uploadThumbnails(WebResourceType thumbnailResource, List<Thumbnail> thumbnails) throws IOException {
        for (Thumbnail thumbnail : thumbnails) {
            if (thumbnail.getTargetName().endsWith(ThumbnailKind.LARGE.getNameSuffix())) {
                LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), thumbnail.getTargetName());
//                s3Client.putObject(thumbnail.getTargetName(), thumbnail.getContentStream(), prepareObjectMetadata(thumbnailResource));
            } else if (thumbnail.getTargetName().endsWith(ThumbnailKind.MEDIUM.getNameSuffix()) &&
                    hasThumbnailResolutionLowerThan200(thumbnailResource)) {
                LOGGER.info("{}\t=>\t{}", thumbnailResource.getAbout(), thumbnail.getTargetName());
//                s3Client.putObject(thumbnail.getTargetName(), thumbnail.getContentStream(), prepareObjectMetadata(thumbnailResource));
            }
        }
    }

    private void appendSuccessReport(WebResourceType thumbnailResource, List<Thumbnail> thumbnails, long elapsedTimeInNanoSec, ReportRow reportRow) {
        reportRow.setHeightBefore(thumbnailResource.getHeight().getLong());
        reportRow.setWidthBefore(thumbnailResource.getWidth().getLong());
        reportRow.setElapsedTime(TimeUnit.SECONDS.convert(elapsedTimeInNanoSec, TimeUnit.NANOSECONDS));
        reportRow.setStatus("PROCESSED");
        for (Thumbnail thumbnail : thumbnails) {
            fileCsvImageReporter.appendRow(getNewWidthAndHeight(thumbnail, reportRow));
        }
    }

    private void appendFailReport(ReportRow reportRow){
        reportRow.setStatus("FAILED");
        fileCsvImageReporter.appendRow(reportRow);
    }

    private ReportRow getNewWidthAndHeight(Thumbnail thumbnail, ReportRow reportRow) {
        try {
            BufferedImage bufferedImage = ImageIO.read(thumbnail.getContentStream());
            reportRow.setHeightAfter(bufferedImage.getHeight());
            reportRow.setWidthAfter(bufferedImage.getWidth());
        } catch (IOException e) {
            LOGGER.error("Cannot get width or height of generated image thumbnail", e);
        }
        return reportRow;
    }

    private List<Thumbnail> generateThumbnails(WebResourceType resource, String thumbnailName, byte[] imageToEnhance) throws IOException {
        File tempImageFile = File.createTempFile(thumbnailName, ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tempImageFile)) {
            fos.write(imageToEnhance);
        } catch (IOException e) {
            LOGGER.error("writing temp file");
            return Collections.emptyList();
        }

        try {
            ThumbnailGenerator thumbnailGenerator = new ThumbnailGenerator(new CommandExecutor(300));
            Pair<ImageMetadata, List<Thumbnail>> output =
                    thumbnailGenerator.generateThumbnails(resource.getAbout(), resource.getHasMimeType().getHasMimeType(), tempImageFile, false);
            Files.deleteIfExists(tempImageFile.toPath());
            return output.getRight();
        } catch (MediaProcessorException | MediaExtractionException e) {
            LOGGER.error("running extracting media", e);
        }

        return Collections.emptyList();
    }

    private boolean hasThumbnailResolutionLowerThan400(WebResourceType thumbnailResource) {
        return thumbnailResource.getHeight() != null && thumbnailResource.getWidth() != null &&
                Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 400;
    }

    private boolean hasThumbnailResolutionLowerThan200(WebResourceType thumbnailResource) {
        return thumbnailResource.getHeight() != null && thumbnailResource.getWidth() != null &&
                Math.min(thumbnailResource.getHeight().getLong(), thumbnailResource.getWidth().getLong()) < 200;
    }

    private ObjectMetadata prepareObjectMetadata(WebResourceType webResource) {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(webResource.getHasMimeType().getHasMimeType());
        objectMetadata.setContentLength(webResource.getFileByteSize().getLong());
        return objectMetadata;
    }
}
