package eu.europeana.metis.reprocessing.execution;

import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.reprocessing.model.BasicConfiguration;
import eu.europeana.metis.schema.jibx.RDF;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains functionality for processing a record.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the processing of records</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-15
 */
public class ProcessingUtilities {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingUtilities.class);

  private ProcessingUtilities() {
  }

  public static RDF processFullBean(FullBeanImpl fullBean, BasicConfiguration basicConfiguration) {
    if (basicConfiguration.isIdentityProcess()) {
      final RDF rdf = EdmUtils.toRDF(fullBean, true);
      //Remove quality annotations because EdmUtils prefixes them with an http domain. They will
      // be recalculated during indexing
      rdf.setQualityAnnotationList(null);
      return rdf;
    } else {
      return process(fullBean);
    }
  }

  private static RDF identityProcess(FullBeanImpl fullBean) {
    return EdmUtils.toRDF(fullBean, true);
  }

  private static RDF process(FullBeanImpl fullBean) {
    // TODO: 24/11/2020 Extend to the implementation that modifies the record
    return null;
  }

  //  /**
  //   * Given a {@link FullBeanImpl} record it re-establishes the technical metadata in the {@link
  //   * RDF}
  //   *
  //   * @param fullBean the source record
  //   * @param basicConfiguration the configuration class that contains required properties
  //   * @return the record containing the technical metadata
  //   * @throws ProcessingException if an exception occurred while processing
  //   */
  //  public static RDF updateTechnicalMetadata(final FullBeanImpl fullBean,
  //      BasicConfiguration basicConfiguration) throws ProcessingException {
  //    try {
  //      final MongoSourceMongoDao mongoSourceMongoDao = basicConfiguration.getMongoSourceMongoDao();
  //      final ExtraConfiguration extraConfiguration = basicConfiguration.getExtraConfiguration();
  //      final RDF rdf = EdmUtils.toRDF(fullBean, true);
  //      final EnrichedRdfImpl enrichedRdf = new EnrichedRdfImpl(rdf);
  //
  //      for (AggregationImpl aggregation : fullBean.getAggregations()) {
  //        //Get all urls that should have webResources
  //        final Stream<String> urlsStream = Stream
  //            .of(aggregation.getEdmObject(), aggregation.getEdmIsShownAt(),
  //                aggregation.getEdmIsShownBy());
  //        final Stream<String> hasViewStream =
  //            aggregation.getHasView() != null ? Arrays.stream(aggregation.getHasView())
  //                : Stream.empty();
  //        List<String> urlsForWebResources = Stream.concat(urlsStream, hasViewStream)
  //            .filter(Objects::nonNull).collect(Collectors.toList());
  //        for (String resourceUrl : urlsForWebResources) {
  //          technicalMetadataForResource(mongoSourceMongoDao, enrichedRdf, resourceUrl,
  //              extraConfiguration.getCacheMongoDao(), extraConfiguration.getAmazonS3Client(),
  //              extraConfiguration.getS3Bucket(), rdf.getProvidedCHOList().get(0).getAbout());
  //        }
  //      }
  //      return enrichedRdf.finalizeRdf();
  //    } catch (Exception e) {
  //      throw new ProcessingException("A Runtime Exception occurred", e);
  //    }
  //  }

  //  /**
  //   * Given an {@link EnrichedRdfImpl} and the resource to be checked for technical metadata
  //   * availability, it will check and {@link EnrichedRdfImpl#enrichResource(ResourceMetadata)} the
  //   * rdf with the resource metadata.
  //   * <p>Each original technical metadata has to be retrieved from the source mongo and re-combined
  //   * in the {@link EnrichedRdfImpl}. If the original technical metadata is not present then the
  //   * cache is checked and if there is technical metadata for that resource then that is inserted in
  //   * the {@link EnrichedRdfImpl}. And if that technincal metadata has thumbnails they are uploaded
  //   * to S3 if not already present.</p>
  //   *
  //   * @param mongoSourceMongoDao the source mongo dao where the original technical metadata reside
  //   * @param enrichedRdf the provided rdf
  //   * @param resourceUrl the resource url to check upon
  //   * @param cacheMongoDao the cache mongo dao where the cached technical metadata reside
  //   * @param amazonS3Client the S3 client for uploading thumbnails
  //   * @param s3Bucket the S3 bucket
  //   */
  //  private static void technicalMetadataForResource(final MongoSourceMongoDao mongoSourceMongoDao,
  //      final EnrichedRdfImpl enrichedRdf, final String resourceUrl,
  //      final CacheMongoDao cacheMongoDao, final AmazonS3 amazonS3Client,
  //      final String s3Bucket, final String recordId) {
  //    try {
  //      final String md5HexTechnicalMetadata = md5Hex(resourceUrl + "-" + recordId);
  //      final String md5HexThumb = md5Hex(resourceUrl);
  //      final WebResourceMetaInfoImpl webResourceMetaInfoImplFromSource = mongoSourceMongoDao
  //          .getTechnicalMetadataFromSource(md5HexTechnicalMetadata);
  //      if (webResourceMetaInfoImplFromSource == null) {
  //        //If it does not exist already check cache
  //        final TechnicalMetadataWrapper technicalMetadataWrapper = cacheMongoDao
  //            .getTechnicalMetadataWrapper(resourceUrl);
  //        if (technicalMetadataWrapper != null && technicalMetadataWrapper.isSuccessExtraction()) {
  //          enrichedRdf.enrichResource(technicalMetadataWrapper.getResourceMetadata());
  //        }
  //      } else {
  //        final ResourceMetadata resourceMetadata = convertWebResourceMetaInfoImpl(amazonS3Client,
  //            s3Bucket, webResourceMetaInfoImplFromSource, resourceUrl, md5HexThumb);
  //        enrichedRdf.enrichResource(resourceMetadata);
  //      }
  //    } catch (MediaExtractionException e) {
  //      LOGGER.warn("Could not enrich with technical metadata of resourceUrl: {}", resourceUrl, e);
  //    }
  //  }

  //  static boolean doesThumbnailExistInS3(AmazonS3 amazonS3Client, String s3Bucket,
  //      String targetNameLarge) {
  //    return amazonS3Client.doesObjectExist(s3Bucket, targetNameLarge);
  //  }

  /**
   * Converts a string to md5 hash.
   *
   * @param stringToMd5 the string to convert
   * @return the md5 hash of the string
   * @throws MediaExtractionException if the md5 could not be generated
   */
  private static String md5Hex(final String stringToMd5) throws MediaExtractionException {
    try {
      byte[] bytes = stringToMd5.getBytes(StandardCharsets.UTF_8.name());
      byte[] md5bytes = MessageDigest.getInstance("MD5").digest(bytes);
      return String.format("%032x", new BigInteger(1, md5bytes));
    } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
      throw new MediaExtractionException("Could not compute md5 hash", e);
    }
  }

  //  /**
  //   * Converted for {@link WebResourceMetaInfoImpl} to {@link ResourceMetadata} classes.
  //   * <p>The conversion is based on records that already exist in the source data. So for each
  //   * resource, there is a check performed, of it's md5 hash, in S3. That check is to generate the
  //   * target names of the thumnails if they were previously present.</p>
  //   *
  //   * @param amazonS3Client the S3 client
  //   * @param s3Bucket the S3 bucket
  //   * @param webResourceMetaInfo the provided web resource metadata for conversion
  //   * @param resourceUrl the resource url that the web resource metadata is based upon
  //   * @param md5Hex the md5 hash of the resource url
  //   * @return the converted class
  //   * @throws MediaExtractionException if a conversion failed
  //   */
  //  static ResourceMetadata convertWebResourceMetaInfoImpl(AmazonS3 amazonS3Client, String s3Bucket,
  //      WebResourceMetaInfoImpl webResourceMetaInfo, String resourceUrl, String md5Hex)
  //      throws MediaExtractionException {
  //    final AudioMetaInfoImpl audioMetaInfo = webResourceMetaInfo.getAudioMetaInfo();
  //    final ImageMetaInfoImpl imageMetaInfo = webResourceMetaInfo.getImageMetaInfo();
  //    final TextMetaInfoImpl textMetaInfo = webResourceMetaInfo.getTextMetaInfo();
  //    final VideoMetaInfoImpl videoMetaInfo = webResourceMetaInfo.getVideoMetaInfo();
  //    ResourceMetadata resourceMetadata = null;
  //
  //    if (audioMetaInfo != null) {
  //      final AudioResourceMetadata audioResourceMetadata = new AudioResourceMetadata(
  //          audioMetaInfo.getMimeType(), resourceUrl, audioMetaInfo.getFileSize(),
  //          audioMetaInfo.getDuration(), audioMetaInfo.getBitRate(), audioMetaInfo.getChannels(),
  //          audioMetaInfo.getSampleRate(), audioMetaInfo.getBitDepth());
  //      resourceMetadata = new ResourceMetadata(audioResourceMetadata);
  //    } else if (imageMetaInfo != null) {
  //      ArrayList<Thumbnail> thumbnails = getThumbnailTargetNames(amazonS3Client, s3Bucket,
  //          resourceUrl, md5Hex);
  //      final List<String> colorPaletteWithoutStartingHash = Optional
  //          .ofNullable(imageMetaInfo.getColorPalette()).map(Arrays::stream).orElseGet(Stream::empty)
  //          .map(color -> color.startsWith("#") ? color.substring(1) : color)
  //          .collect(Collectors.toList());
  //      final ImageResourceMetadata imageResourceMetadata = new ImageResourceMetadata(
  //          imageMetaInfo.getMimeType(), resourceUrl, imageMetaInfo.getFileSize(),
  //          imageMetaInfo.getWidth(), imageMetaInfo.getHeight(),
  //          ColorSpaceType.convert(imageMetaInfo.getColorSpace()),
  //          colorPaletteWithoutStartingHash, thumbnails);
  //      resourceMetadata = new ResourceMetadata(imageResourceMetadata);
  //    } else if (textMetaInfo != null) {
  //      ArrayList<Thumbnail> thumbnails = getThumbnailTargetNames(amazonS3Client, s3Bucket,
  //          resourceUrl, md5Hex);
  //      final TextResourceMetadata textResourceMetadata = new TextResourceMetadata(
  //          textMetaInfo.getMimeType(), resourceUrl, textMetaInfo.getFileSize(),
  //          Optional.ofNullable(textMetaInfo.getIsSearchable()).orElse(false),
  //          textMetaInfo.getResolution(), thumbnails);
  //      resourceMetadata = new ResourceMetadata(textResourceMetadata);
  //    } else if (videoMetaInfo != null) {
  //      final VideoResourceMetadata videoResourceMetadata = new VideoResourceMetadata(
  //          videoMetaInfo.getMimeType(), resourceUrl, videoMetaInfo.getFileSize(),
  //          videoMetaInfo.getDuration(), videoMetaInfo.getBitRate(), videoMetaInfo.getWidth(),
  //          videoMetaInfo.getHeight(), videoMetaInfo.getCodec(), videoMetaInfo.getFrameRate());
  //      resourceMetadata = new ResourceMetadata(videoResourceMetadata);
  //    }
  //    return resourceMetadata;
  //  }
  //
  //  private static ArrayList<Thumbnail> getThumbnailTargetNames(AmazonS3 amazonS3Client,
  //      String s3Bucket, String resourceUrl, String md5Hex) {
  //    // TODO: 11-7-19 Better way to do this? To avoid checking S3..
  //    ArrayList<Thumbnail> thumbnails = new ArrayList<>();
  //    String targetNameLarge = md5Hex + "-LARGE";
  //    LOGGER.info("Get thumbnail target names existence from S3");
  //    if (doesThumbnailExistInS3(amazonS3Client, s3Bucket, targetNameLarge)) {
  //      final ThumbnailImpl thumbnail = new ThumbnailImpl(resourceUrl, targetNameLarge);
  //      //Close immediately, the contained files are not required.
  //      thumbnail.close();
  //      thumbnails.add(thumbnail);
  //    }
  //    return thumbnails;
  //  }
}
