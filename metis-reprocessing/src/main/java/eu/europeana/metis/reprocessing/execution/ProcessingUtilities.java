package eu.europeana.metis.reprocessing.execution;

import eu.europeana.corelib.definitions.jibx.ColorSpaceType;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.edm.model.metainfo.AudioMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.ImageMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.TextMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.VideoMetaInfoImpl;
import eu.europeana.corelib.edm.model.metainfo.WebResourceMetaInfoImpl;
import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.AggregationImpl;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.AudioResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.EnrichedRdfImpl;
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
import java.util.Arrays;
import java.util.List;
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

  static RDF updateTechnicalMetadata(final FullBeanImpl fullBean, final MongoDao mongoDao) {
    final RDF rdf = EdmUtils.toRDF(fullBean);
    final EnrichedRdfImpl enrichedRdf = new EnrichedRdfImpl(rdf);

    for (AggregationImpl aggregation : fullBean.getAggregations()) {
      //Get all urls that should have webResources
      List<String> urlsForWebResources = Stream
          .of(aggregation.getEdmObject(), aggregation.getEdmIsShownAt(),
              aggregation.getEdmIsShownBy()).collect(Collectors.toList());

      for (String resourceUrl : urlsForWebResources) {
        technicalMetadataForResource(mongoDao, enrichedRdf, resourceUrl);
      }
    }
    return enrichedRdf.finalizeRdf();
  }

  private static void technicalMetadataForResource(final MongoDao mongoDao,
      final EnrichedRdfImpl enrichedRdf, final String resourceUrl) {
    try {
      final String md5Hex = ProcessingUtilities.md5Hex(resourceUrl);
      final WebResourceMetaInfoImpl webResourceMetaInfoImplFromSource = mongoDao
          .getTechnicalMetadataFromSource(md5Hex);
      if (webResourceMetaInfoImplFromSource == null) {
        //If it does not exist already check cache
        final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
            .getTechnicalMetadataWrapper(resourceUrl);
        if (technicalMetadataWrapper != null) {
          enrichedRdf.enrichResource(technicalMetadataWrapper.getResourceMetadata());
          //Get all Thumbnails and store to S3
        }
      } else {
        final ResourceMetadata resourceMetadata = convertWebResourceMetaInfoImpl(
            webResourceMetaInfoImplFromSource, resourceUrl);
        enrichedRdf.enrichResource(resourceMetadata);
      }
    } catch (MediaExtractionException e) {
      LOGGER.warn("Could not enrich with technical metadata of resourceUrl: {}", resourceUrl, e);
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

  static ResourceMetadata convertWebResourceMetaInfoImpl(
      WebResourceMetaInfoImpl webResourceMetaInfo, String resourceUrl)
      throws MediaExtractionException {
    final AudioMetaInfoImpl audioMetaInfo = webResourceMetaInfo.getAudioMetaInfo();
    final ImageMetaInfoImpl imageMetaInfo = webResourceMetaInfo.getImageMetaInfo();
    final TextMetaInfoImpl textMetaInfo = webResourceMetaInfo.getTextMetaInfo();
    final VideoMetaInfoImpl videoMetaInfo = webResourceMetaInfo.getVideoMetaInfo();
    ResourceMetadata resourceMetadata = null;

    if (audioMetaInfo != null) {
      final AudioResourceMetadata audioResourceMetadata = new AudioResourceMetadata(
          audioMetaInfo.getMimeType(), resourceUrl, audioMetaInfo.getFileSize(),
          audioMetaInfo.getDuration(), audioMetaInfo.getBitRate(), audioMetaInfo.getChannels(),
          audioMetaInfo.getSampleRate(), audioMetaInfo.getBitDepth());
      resourceMetadata = new ResourceMetadata(audioResourceMetadata);
    } else if (imageMetaInfo != null) {
      // TODO: 16-5-19 Create Thumbnail with -LARGE target name if exists
//      final ThumbnailImpl thumbnail = new ThumbnailImpl(resourceUrl, "");
      final ImageResourceMetadata imageResourceMetadata = new ImageResourceMetadata(
          imageMetaInfo.getMimeType(), resourceUrl, imageMetaInfo.getFileSize(),
          imageMetaInfo.getWidth(), imageMetaInfo.getHeight(),
          ColorSpaceType.convert(imageMetaInfo.getColorSpace()),
          Arrays.asList(imageMetaInfo.getColorPalette()), null);
      resourceMetadata = new ResourceMetadata(imageResourceMetadata);
    } else if (textMetaInfo != null) {
      // TODO: 16-5-19 Create Thumbnail with -LARGE target name if exists
      final TextResourceMetadata textResourceMetadata = new TextResourceMetadata(
          textMetaInfo.getMimeType(), resourceUrl, textMetaInfo.getFileSize(),
          textMetaInfo.getIsSearchable(), textMetaInfo.getResolution(), null);
      resourceMetadata = new ResourceMetadata(textResourceMetadata);
    } else if (videoMetaInfo != null) {
      final VideoResourceMetadata videoResourceMetadata = new VideoResourceMetadata(
          videoMetaInfo.getMimeType(), resourceUrl, videoMetaInfo.getFileSize(),
          videoMetaInfo.getDuration(), videoMetaInfo.getBitRate(), videoMetaInfo.getWidth(),
          videoMetaInfo.getHeight(), videoMetaInfo.getCodec(), videoMetaInfo.getFrameRate());
      resourceMetadata = new ResourceMetadata(videoResourceMetadata);
    }
    return resourceMetadata;
  }
}
