package eu.europeana.metis.reprocessing.execution;

import eu.europeana.corelib.definitions.edm.entity.WebResource;
import eu.europeana.corelib.definitions.edm.model.metainfo.ImageOrientation;
import eu.europeana.corelib.definitions.jibx.ColorSpaceType;
import eu.europeana.corelib.edm.model.metainfo.AudioMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.ImageMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.TextMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.VideoMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.AggregationImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.AbstractResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.AudioResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.ImageResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.ResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.TextResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.VideoResourceMetadata;
import eu.europeana.metis.reprocessing.model.TechnicalMetadataWrapper;
import eu.europeana.metis.reprocessing.utilities.MongoDao;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-15
 */
public class ProcessingUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingUtilities.class);

  private ProcessingUtilities() {
  }

  static void updateTechnicalMetadata(final FullBeanImpl fullBean, final MongoDao mongoDao) {
    for (AggregationImpl aggregation : fullBean.getAggregations()) {
      //Get all urls that should have webResources
      List<String> urlsForWebResources = Stream
          .of(aggregation.getEdmObject(), aggregation.getEdmIsShownAt(),
              aggregation.getEdmIsShownBy()).collect(Collectors.toList());

      for (String resourceUrl : urlsForWebResources) {
        technicalMetadataForResource(mongoDao, aggregation, resourceUrl);
      }
    }
  }

  private static void technicalMetadataForResource(final MongoDao mongoDao,
      final AggregationImpl aggregation, final String resourceUrl) {
    try {
      final String md5Hex = ProcessingUtilities.md5Hex(resourceUrl);
      if (!mongoDao.doTechnicalMetadataExistInSource(md5Hex)) {
        //If it does not exist already check cache
        final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
            .getTechnicalMetadataWrapper(resourceUrl);
        if (technicalMetadataWrapper != null) {
          //Convert technical metadata to WebResourceMetaInfo
          final WebResourceMetaInfoImpl webresourceMetaInfo = ProcessingUtilities
              .createWebresourceMetaInfo(md5Hex,
                  technicalMetadataWrapper.getResourceMetadata());
          //Create WebResource in record
          createWebResourceInAggregationIfNotExistent(aggregation, resourceUrl);
          //Store technical metadata to database.
          //Get all Thumbnails and store to S3
        }
      } else {
        //If technical metadata exist, add the web resource in case it doesn't already exist
        createWebResourceInAggregationIfNotExistent(aggregation, resourceUrl);
      }
    } catch (MediaExtractionException e) {
      LOGGER.warn("MD5 Hash for resourceUrl could not be generated: {}", resourceUrl, e);
    }
  }

  static void tierCalculation(FullBeanImpl fullBean) {
  }

  private static String md5Hex(final String stringToMd5) throws MediaExtractionException {
    try {
      byte[] bytes = stringToMd5.getBytes(StandardCharsets.UTF_8.name());
      byte[] md5bytes = MessageDigest.getInstance("MD5").digest(bytes);
      return String.format("%032x", new BigInteger(1, md5bytes));
    } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
      throw new MediaExtractionException("Could not compute md5 hash", e);
    }
  }

  static void createWebResourceInAggregationIfNotExistent(final AggregationImpl aggregation,
      final String resourceUrl) {
    final List<WebResourceImpl> webResources = castWebResourceList(aggregation.getWebResources());
    if (Objects.requireNonNull(webResources).stream()
        .noneMatch(c -> c.getAbout().equals(resourceUrl))) {
      final WebResourceImpl webResource = new WebResourceImpl();
      webResource.setAbout(resourceUrl);
      webResources.add(webResource);
    }
  }

  static WebResourceMetaInfoImpl createWebresourceMetaInfo(final String md5Hex,
      final ResourceMetadata resourceMetadata) {
    final WebResourceMetaInfoImpl webResourceMetaInfoImpl = new WebResourceMetaInfoImpl(md5Hex,
        null, null, null, null);
    final AbstractResourceMetadata metaData = resourceMetadata.getMetaData();
    if (metaData instanceof AudioResourceMetadata) {
      AudioResourceMetadata audioResourceMetadata = (AudioResourceMetadata) metaData;
      AudioMetaInfoImpl metaInfo = new AudioMetaInfoImpl();

      metaInfo.setMimeType(audioResourceMetadata.getMimeType());
      metaInfo.setFileSize(audioResourceMetadata.getContentSize());

      metaInfo.setDuration((long) audioResourceMetadata.getDuration());
      metaInfo.setSampleRate(audioResourceMetadata.getSampleRate());
      metaInfo.setBitRate(audioResourceMetadata.getBitRate());
      metaInfo.setBitDepth(audioResourceMetadata.getSampleSize());
      metaInfo.setChannels(audioResourceMetadata.getChannels());

      webResourceMetaInfoImpl.setAudioMetaInfo(metaInfo);
    } else if (metaData instanceof ImageResourceMetadata) {
      ImageResourceMetadata imageResourceMetadata = (ImageResourceMetadata) metaData;
      ImageMetaInfoImpl metaInfo = new ImageMetaInfoImpl();

      metaInfo.setMimeType(imageResourceMetadata.getMimeType());
      metaInfo.setFileSize(imageResourceMetadata.getContentSize());

      metaInfo.setHeight(imageResourceMetadata.getHeight());
      metaInfo.setWidth(imageResourceMetadata.getWidth());

      final Function<ColorSpaceType, String> colorSpaceToString = value ->
          value == ColorSpaceType.GRAYSCALE ? "Gray" : value.xmlValue();
      metaInfo.setColorSpace(
          Optional.ofNullable(imageResourceMetadata.getColorSpace()).map(colorSpaceToString)
              .orElse(null));
      final String[] targetColors = imageResourceMetadata.getDominantColors()
          .toArray(new String[0]);
      metaInfo.setColorPalette(targetColors.length == 0 ? null : targetColors);

      final ImageOrientation targetOrientation =
          imageResourceMetadata.getWidth() > imageResourceMetadata.getHeight()
              ? ImageOrientation.LANDSCAPE : ImageOrientation.PORTRAIT;
      metaInfo.setOrientation(targetOrientation);

      webResourceMetaInfoImpl.setImageMetaInfo(metaInfo);
    } else if (metaData instanceof TextResourceMetadata) {
      TextResourceMetadata textResourceMetadata = (TextResourceMetadata) metaData;
      TextMetaInfoImpl metaInfo = new TextMetaInfoImpl();

      metaInfo.setMimeType(textResourceMetadata.getMimeType());
      metaInfo.setFileSize(textResourceMetadata.getContentSize());
      metaInfo.setResolution(textResourceMetadata.getResolution());
      metaInfo.setRdfType(textResourceMetadata.getResourceUrl());

      webResourceMetaInfoImpl.setTextMetaInfo(metaInfo);
    } else if (metaData instanceof VideoResourceMetadata) {
      VideoResourceMetadata videoResourceMetadata = (VideoResourceMetadata) metaData;
      VideoMetaInfoImpl metaInfo = new VideoMetaInfoImpl();

      metaInfo.setMimeType(videoResourceMetadata.getMimeType());
      metaInfo.setFileSize(videoResourceMetadata.getContentSize());

      metaInfo.setCodec(videoResourceMetadata.getCodecName());
      metaInfo.setWidth(videoResourceMetadata.getWidth());
      metaInfo.setHeight(videoResourceMetadata.getHeight());
      metaInfo.setBitRate(videoResourceMetadata.getBitRate());
      metaInfo.setFrameRate(videoResourceMetadata.getFrameRate());
      metaInfo.setDuration((long) videoResourceMetadata.getDuration());

      webResourceMetaInfoImpl.setVideoMetaInfo(metaInfo);
    }
    return webResourceMetaInfoImpl;
  }

  private static List<WebResourceImpl> castWebResourceList(
      final List<? extends WebResource> input) {
    return input == null ? null
        : input.stream().map(webResource -> ((WebResourceImpl) webResource))
            .collect(Collectors.toList());
  }
}
