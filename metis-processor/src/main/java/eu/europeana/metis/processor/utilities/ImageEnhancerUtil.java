package eu.europeana.metis.processor.utilities;

import static eu.europeana.metis.mediaprocessing.extraction.ThumbnailGenerator.md5Hex;
import static eu.europeana.metis.network.ExternalRequestUtil.retryableExternalRequest;

import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.indexing.utils.RdfWrapper;
import eu.europeana.indexing.utils.WebResourceLinkType;
import eu.europeana.indexing.utils.WebResourceWrapper;
import eu.europeana.metis.image.enhancement.domain.worker.ImageEnhancerWorker;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.extraction.CommandExecutor;
import eu.europeana.metis.mediaprocessing.extraction.ImageMetadata;
import eu.europeana.metis.mediaprocessing.extraction.ThumbnailGenerator;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.ThumbnailKind;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.processor.config.Mode;
import eu.europeana.metis.schema.jibx.Aggregation;
import eu.europeana.metis.schema.jibx.RDF;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.imageio.ImageIO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Enhancement processor.
 */
public class ImageEnhancerUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImageEnhancerUtil.class);
  private static final Map<Class<?>, String> retryExceptions;

  static {
    retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
    retryExceptions.put(NoHttpResponseException.class, "");
  }

  private final S3Client s3Client;
  private final ImageEnhancerWorker imageEnhancerWorker;
  private final FileCsvImageReporter fileCsvImageReporter;
  private final Mode proccesingMode;

  /**
   * Instantiates a new Enhancement processor.
   *
   * @param s3Client the s3 client
   * @param imageEnhancerWorker the image enhancer client
   */
  public ImageEnhancerUtil(S3Client s3Client,
      ImageEnhancerWorker imageEnhancerWorker,
      FileCsvImageReporter fileCsvImageReporter,
      Mode processingMode) {
    this.s3Client = s3Client;
    this.imageEnhancerWorker = imageEnhancerWorker;
    this.fileCsvImageReporter = fileCsvImageReporter;
    this.proccesingMode = processingMode;
  }

  /**
   * Process record.
   *
   * @param recordToProcess the record to process
   */
  public void processRecord(RDF recordToProcess) {
    extractThumbnailEnhanceAndReUploadIt(recordToProcess);
  }

  private void extractThumbnailEnhanceAndReUploadIt(RDF recordToProcess) {
    List<Aggregation> curatedAggregationList = Optional.ofNullable(recordToProcess.getAggregationList())
                                                       .orElse(Collections.emptyList());
    final String recordId = recordToProcess.getProvidedCHOList().get(0).getAbout();
    if (hasAggregationWithHasView(curatedAggregationList)) {
      LOGGER.info("=>hasView");
      curatedAggregationList.forEach(aggregationItem -> {
            if (hasIsShownBy(aggregationItem)) {
              LOGGER.info("=>shownByItem");
              processWebResourceEntry(WebResourceLinkType.IS_SHOWN_BY, recordToProcess, recordId);
            }
            LOGGER.info("=>hasViewItem");
            processWebResourceEntry(WebResourceLinkType.HAS_VIEW, recordToProcess, recordId);
          }
      );
    }
      // else {
      // this is just for backwards compatibilty Batch A
      // processWebResourceEntry(WebResourceLinkType.IS_SHOWN_BY, recordToProcess, recordId);
      // }
  }

  private void processWebResourceEntry(WebResourceLinkType linkType, RDF recordToProcess, String recordId) {
    List<WebResourceWrapper> webResourceWrappers = new RdfWrapper(recordToProcess)
        .getWebResourceWrappers(Set.of(linkType))
        .stream()
        .filter(webResourceWrapper -> webResourceWrapper != null
            && webResourceWrapper.getMimeType() != null
            && webResourceWrapper.getMimeType().startsWith("image/"))
        .distinct()
        .collect(Collectors.toList());
    webResourceWrappers.forEach(webResourceWrapper -> {
      LOGGER.info("=>SimpleItem{} {}", linkType, webResourceWrapper.getAbout());
      processItem(recordId, webResourceWrapper);
    });
  }

  private void processItem(String recordId, WebResourceWrapper webResourceWrapper) {
    ReportRow reportRow = new ReportRow();
    reportRow.setRecordId(recordId);
    reportRow.setImageLink(webResourceWrapper.getAbout());
    if (!proccesingMode.equals(Mode.DRY_RUN)) {
      try {
        if (hasThumbnailResolutionLowerThan400(webResourceWrapper)) {
          final String thumbnailHex = md5Hex(webResourceWrapper.getAbout());
          final String largeThumbnailObjectName = thumbnailHex + ThumbnailKind.LARGE.getNameSuffix();
          LOGGER.debug("{}\t=>\t{}", webResourceWrapper.getAbout(), largeThumbnailObjectName);
          byte[] largeThumbnail = s3Client.getIbmObject(largeThumbnailObjectName);

          if (ArrayUtils.isEmpty(largeThumbnail)) {
            largeThumbnail = s3Client.getAmazonObject(largeThumbnailObjectName);
          }
          if (isWebResourceResolutionAndImageResolutionSame(webResourceWrapper, largeThumbnail)) {
            //                        Files.write(Paths.get("/tmp/test_thumbnails/" + largeThumbnailObjectName), largeThumbnail);
            final long startTime = System.nanoTime();
            byte[] enhancedImage = enhanceImage(largeThumbnail, largeThumbnailObjectName);
            //                        Files.write(Paths.get("/tmp/test_thumbnails/enhanced-" + largeThumbnailObjectName), enhancedImage);
            final long elapsedTimeInNanoSec = System.nanoTime() - startTime;

            List<Thumbnail> thumbnails = generateThumbnails(webResourceWrapper, largeThumbnailObjectName,
                enhancedImage);
            uploadThumbnails(webResourceWrapper, thumbnails);
            appendSuccessReport(webResourceWrapper, enhancedImage, thumbnailHex, elapsedTimeInNanoSec, reportRow);
          } else {
            appendProcessedReport(webResourceWrapper, thumbnailHex, reportRow);
          }
        } else {
          appendUnProcessedReport(webResourceWrapper, "", reportRow);
        }
      } catch (MediaExtractionException | IOException e) {
        LOGGER.error("Failed enhancing thumbnail image {} {}", webResourceWrapper.getAbout(), e);
        appendFailReport(reportRow);
      }
    } else {
      try {
        appendDryRunReport(webResourceWrapper, md5Hex(webResourceWrapper.getAbout()), reportRow);
      } catch (MediaExtractionException e) {
        LOGGER.error("Failed dry run {} {}", webResourceWrapper.getAbout(), e);
        appendFailReport(reportRow);
      }
    }
  }

  private boolean isWebResourceResolutionAndImageResolutionSame(WebResourceWrapper webResourceWrapper, byte[] imageBytes)
      throws IOException {
    final BufferedImage largeThumbnailBufferedImage = getBufferedImage(imageBytes);
    if (largeThumbnailBufferedImage == null) {
      throw new IOException("Could not get resolution from provided image");
    }
    return largeThumbnailBufferedImage.getHeight() == webResourceWrapper.getHeight() &&
        largeThumbnailBufferedImage.getWidth() == webResourceWrapper.getWidth();
  }

  private byte[] enhanceImage(byte[] largeThumbnail, String largeThumbnailObjectName) {
    if (proccesingMode.equals(Mode.DEFAULT)) {
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
    } else {
      return largeThumbnail;
    }
  }

  private void uploadThumbnails(WebResourceWrapper thumbnailResource, List<Thumbnail> thumbnails) throws IOException {
    for (Thumbnail thumbnail : thumbnails) {
      if (thumbnail.getTargetName().endsWith(ThumbnailKind.LARGE.getNameSuffix()) ||
          (thumbnail.getTargetName().endsWith(ThumbnailKind.MEDIUM.getNameSuffix()) &&
              hasThumbnailResolutionLowerThan200(thumbnailResource))) {
        LOGGER.debug("{}\t=>\t{}", thumbnailResource.getAbout(), thumbnail.getTargetName());
        //                Files.write(Paths.get("/tmp/test_thumbnails/newThumbnail-" + thumbnail.getTargetName()), thumbnail.getContentStream().readAllBytes());
        if (proccesingMode.equals(Mode.DEFAULT)) {
          s3Client.putIbmObject(thumbnail.getTargetName(), thumbnail.getContentStream(),
              prepareObjectMetadata(thumbnailResource.getMimeType(), thumbnail.getContentSize()));
        }
      }
    }
  }

  private void appendSuccessReport(WebResourceWrapper thumbnailResource, byte[] enhancedImage, String thumbnailHex,
      long elapsedTimeInNanoSec, ReportRow reportRow) {
    reportRow.setHeightBefore(thumbnailResource.getHeight());
    reportRow.setWidthBefore(thumbnailResource.getWidth());
    reportRow.setElapsedTime(TimeUnit.MILLISECONDS.convert(elapsedTimeInNanoSec, TimeUnit.NANOSECONDS));
    reportRow.setImageLinkHex(thumbnailHex);
    appendNewWidthAndHeight(enhancedImage, reportRow);
    reportRow.setStatus("SUCCESS");
    fileCsvImageReporter.appendRow(reportRow);
  }

  private void appendProcessedReport(WebResourceWrapper thumbnailResource, String thumbnailHex, ReportRow reportRow) {
    reportRow.setHeightBefore(thumbnailResource.getHeight());
    reportRow.setWidthBefore(thumbnailResource.getWidth());
    reportRow.setImageLinkHex(thumbnailHex);
    reportRow.setStatus("ALREADYPROCESSED");
    fileCsvImageReporter.appendRow(reportRow);
  }

  private void appendDryRunReport(WebResourceWrapper thumbnailResource, String thumbnailHex, ReportRow reportRow) {
    reportRow.setHeightBefore(thumbnailResource.getHeight());
    reportRow.setWidthBefore(thumbnailResource.getWidth());
    reportRow.setImageLinkHex(thumbnailHex);
    reportRow.setStatus("DRYRUN");
    fileCsvImageReporter.appendRow(reportRow);
  }

  private void appendUnProcessedReport(WebResourceWrapper thumbnailResource, String thumbnailHex, ReportRow reportRow) {
    reportRow.setHeightBefore(thumbnailResource.getHeight());
    reportRow.setWidthBefore(thumbnailResource.getWidth());
    reportRow.setImageLinkHex(thumbnailHex);
    reportRow.setStatus("UNPROCESSED");
    fileCsvImageReporter.appendRow(reportRow);
  }

  private void appendFailReport(ReportRow reportRow) {
    reportRow.setStatus("FAIL");
    fileCsvImageReporter.appendRow(reportRow);
  }

  private BufferedImage getBufferedImage(byte[] image) {
    BufferedImage bufferedImage = null;
    try {
      bufferedImage = ImageIO.read(new ByteArrayInputStream(image));
    } catch (IOException e) {
      LOGGER.error("Cannot get width or height of generated image thumbnail", e);
    }
    return bufferedImage;
  }

  private void appendNewWidthAndHeight(byte[] image, ReportRow reportRow) {
    final BufferedImage bufferedImage = getBufferedImage(image);
    if (bufferedImage != null) {
      reportRow.setHeightAfter(bufferedImage.getHeight());
      reportRow.setWidthAfter(bufferedImage.getWidth());
    }
  }

  private List<Thumbnail> generateThumbnails(WebResourceWrapper resource, String thumbnailName, byte[] imageToEnhance)
      throws IOException {
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
          thumbnailGenerator.generateThumbnails(resource.getAbout(), resource.getMimeType(), tempImageFile, false);
      Files.deleteIfExists(tempImageFile.toPath());
      return output.getRight();
    } catch (MediaProcessorException | MediaExtractionException e) {
      LOGGER.error("running extracting media", e);
    }

    return Collections.emptyList();
  }

  private boolean hasThumbnailResolutionLowerThan400(WebResourceWrapper thumbnailResource) {
    return Math.min(thumbnailResource.getHeight(), thumbnailResource.getWidth()) < 400;
  }

  private boolean hasThumbnailResolutionLowerThan200(WebResourceWrapper thumbnailResource) {
    return Math.min(thumbnailResource.getHeight(), thumbnailResource.getWidth()) < 200;
  }

  private boolean hasIsShownBy(Aggregation aggregation) {
    return aggregation != null && aggregation.getIsShownBy() != null;
  }

  private boolean hasAggregationWithHasView(List<Aggregation> curatedAggregationList) {
    return curatedAggregationList.stream()
                                 .anyMatch(aggregation ->
                                     CollectionUtils.isNotEmpty(aggregation.getHasViewList()));
  }

  private ObjectMetadata prepareObjectMetadata(String contentType, long contentLength) {
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentType(contentType);
    objectMetadata.setContentLength(contentLength);
    return objectMetadata;
  }
}
