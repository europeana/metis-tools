package eu.europeana.metis.reprocessing.execution;

import com.amazonaws.services.s3.AmazonS3;
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
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.ThumbnailImpl;
import eu.europeana.metis.mediaprocessing.model.VideoResourceMetadata;
import eu.europeana.metis.reprocessing.dao.CacheMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.reprocessing.model.ExtraConfiguration;
import eu.europeana.metis.reprocessing.model.TechnicalMetadataWrapper;
import eu.europeana.metis.reprocessing.model.ThumbnailWrapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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

  public static RDF updateTechnicalMetadata(final FullBeanImpl fullBean,
      BasicConfiguration basicConfiguration) throws ProcessingException {
    try {
      final MongoSourceMongoDao mongoSourceMongoDao = basicConfiguration.getMongoSourceMongoDao();
      final ExtraConfiguration extraConfiguration = basicConfiguration.getExtraConfiguration();
      final RDF rdf = EdmUtils.toRDF(fullBean);
      final EnrichedRdfImpl enrichedRdf = new EnrichedRdfImpl(rdf);

      for (AggregationImpl aggregation : fullBean.getAggregations()) {
        //Get all urls that should have webResources
        final Stream<String> urlsStream = Stream
            .of(aggregation.getEdmObject(), aggregation.getEdmIsShownAt(),
                aggregation.getEdmIsShownBy());
        final Stream<String> hasViewStream =
            aggregation.getHasView() != null ? Arrays.stream(aggregation.getHasView())
                : Stream.empty();
        List<String> urlsForWebResources = Stream.concat(urlsStream, hasViewStream)
            .filter(Objects::nonNull).collect(Collectors.toList());
        for (String resourceUrl : urlsForWebResources) {
          technicalMetadataForResource(mongoSourceMongoDao, enrichedRdf, resourceUrl,
              extraConfiguration.getCacheMongoDao(), extraConfiguration.getAmazonS3Client(),
              extraConfiguration.getS3Bucket());
        }
      }
      return enrichedRdf.finalizeRdf();
    } catch (RuntimeException e) {
      throw new ProcessingException("A Runtime Exception occurred", e);
    }
  }

  private static void technicalMetadataForResource(final MongoSourceMongoDao mongoSourceMongoDao,
      final EnrichedRdfImpl enrichedRdf, final String resourceUrl,
      final CacheMongoDao cacheMongoDao, final AmazonS3 amazonS3Client,
      final String s3Bucket) {
    try {
      final String md5Hex = ProcessingUtilities.md5Hex(resourceUrl);
      final WebResourceMetaInfoImpl webResourceMetaInfoImplFromSource = mongoSourceMongoDao
          .getTechnicalMetadataFromSource(md5Hex);
      if (webResourceMetaInfoImplFromSource == null) {
        //If it does not exist already check cache
        final TechnicalMetadataWrapper technicalMetadataWrapper = cacheMongoDao
            .getTechnicalMetadataWrapper(resourceUrl);
        if (technicalMetadataWrapper != null) {
          enrichedRdf.enrichResource(technicalMetadataWrapper.getResourceMetadata());
          storeThumbnailsToS3(amazonS3Client, s3Bucket,
              technicalMetadataWrapper.getThumbnailWrappers());
        }
      } else {
        final ResourceMetadata resourceMetadata = convertWebResourceMetaInfoImpl(amazonS3Client,
            s3Bucket, webResourceMetaInfoImplFromSource, resourceUrl, md5Hex);
        enrichedRdf.enrichResource(resourceMetadata);
      }
    } catch (MediaExtractionException | IOException e) {
      LOGGER.warn("Could not enrich with technical metadata of resourceUrl: {}", resourceUrl, e);
    }
  }

  static boolean doesThumbnailExistInS3(AmazonS3 amazonS3Client, String s3Bucket,
      String targetNameLarge) {
    return amazonS3Client.doesObjectExist(s3Bucket, targetNameLarge);
  }

  static void storeThumbnailsToS3(AmazonS3 amazonS3Client, String s3Bucket,
      List<ThumbnailWrapper> thumbnailWrappers) {
    for (ThumbnailWrapper thumbnailWrapper : thumbnailWrappers) {
      //If the thumbnail already exists(e.g. from a previous execution of the script), avoid sending it again
      if (!doesThumbnailExistInS3(amazonS3Client, s3Bucket, thumbnailWrapper.getTargetName())) {
        try (InputStream stream = new ByteArrayInputStream(thumbnailWrapper.getThumbnailBytes())) {
          amazonS3Client.putObject(s3Bucket, thumbnailWrapper.getTargetName(), stream, null);
        } catch (Exception e) {
          LOGGER.error(
              "Error while uploading {} to S3 in Bluemix. The full error message is: {} because of: ",
              thumbnailWrapper.getTargetName(), e);
        }
      }
    }
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

  static ResourceMetadata convertWebResourceMetaInfoImpl(AmazonS3 amazonS3Client, String s3Bucket,
      WebResourceMetaInfoImpl webResourceMetaInfo, String resourceUrl, String md5Hex)
      throws MediaExtractionException, IOException {
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
      ArrayList<Thumbnail> thumbnails = getThumbnailTargetNames(amazonS3Client, s3Bucket,
          resourceUrl, md5Hex);
      final ImageResourceMetadata imageResourceMetadata = new ImageResourceMetadata(
          imageMetaInfo.getMimeType(), resourceUrl, imageMetaInfo.getFileSize(),
          imageMetaInfo.getWidth(), imageMetaInfo.getHeight(),
          ColorSpaceType.convert(imageMetaInfo.getColorSpace()),
          Arrays.asList(imageMetaInfo.getColorPalette()), thumbnails);
      resourceMetadata = new ResourceMetadata(imageResourceMetadata);
    } else if (textMetaInfo != null) {
      ArrayList<Thumbnail> thumbnails = getThumbnailTargetNames(amazonS3Client, s3Bucket,
          resourceUrl, md5Hex);
      final TextResourceMetadata textResourceMetadata = new TextResourceMetadata(
          textMetaInfo.getMimeType(), resourceUrl, textMetaInfo.getFileSize(),
          textMetaInfo.getIsSearchable(), textMetaInfo.getResolution(), thumbnails);
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

  private static ArrayList<Thumbnail> getThumbnailTargetNames(AmazonS3 amazonS3Client,
      String s3Bucket, String resourceUrl, String md5Hex) throws IOException {
    ArrayList<Thumbnail> thumbnails = new ArrayList<>();
    String targetNameLarge = md5Hex + "-LARGE";
    if (doesThumbnailExistInS3(amazonS3Client, s3Bucket, targetNameLarge)) {
      final ThumbnailImpl thumbnail = new ThumbnailImpl(resourceUrl, targetNameLarge);
      thumbnail.close();
      thumbnails.add(thumbnail);
    }
    return thumbnails;
  }
}
