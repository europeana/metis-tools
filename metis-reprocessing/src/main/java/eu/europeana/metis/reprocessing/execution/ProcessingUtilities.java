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
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.ReindexToPreviewPlugin;
import eu.europeana.metis.core.workflow.plugins.ReindexToPreviewPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.ReindexToPublishPlugin;
import eu.europeana.metis.core.workflow.plugins.ReindexToPublishPluginMetadata;
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
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailWrapper;
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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.types.ObjectId;
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

  /**
   * Given a {@link FullBeanImpl} record it re-establishes the technical metadata in the {@link
   * RDF}
   *
   * @param fullBean the source record
   * @param basicConfiguration the configuration class that contains required properties
   * @return the record containing the technical metadata
   * @throws ProcessingException if an exception occurred while processing
   */
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

  /**
   * Given an {@link EnrichedRdfImpl} and the resource to be checked for technical metadata
   * availability, it will check and {@link EnrichedRdfImpl#enrichResource(ResourceMetadata)} the
   * rdf with the resource metadata.
   * <p>Each original technical metadata has to be retrieved from the source mongo and re-combined
   * in the {@link EnrichedRdfImpl}. If the original technical metadata is not present then the
   * cache is checked and if there is technical metadata for that resource then that is inserted in
   * the {@link EnrichedRdfImpl}. And if that technincal metadata has thumbnails they are uploaded
   * to S3 if not already present.</p>
   *
   * @param mongoSourceMongoDao the source mongo dao where the original technical metadata reside
   * @param enrichedRdf the provided rdf
   * @param resourceUrl the resource url to check upon
   * @param cacheMongoDao the cache mongo dao where the cached technical metadata reside
   * @param amazonS3Client the S3 client for uploading thumbnails
   * @param s3Bucket the S3 bucket
   */
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
        if (technicalMetadataWrapper != null && technicalMetadataWrapper.isSuccessExtraction()) {
          enrichedRdf.enrichResource(technicalMetadataWrapper.getResourceMetadata());
          storeThumbnailsToS3(amazonS3Client, s3Bucket,
              technicalMetadataWrapper.getThumbnailWrappers() == null ? Collections.emptyList()
                  : technicalMetadataWrapper.getThumbnailWrappers());
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

  /**
   * Converted for {@link WebResourceMetaInfoImpl} to {@link ResourceMetadata} classes.
   * <p>The conversion is based on records that already exist in the source data. So for each
   * resource, there is a check performed, of it's md5 hash, in S3. That check is to generate the
   * target names of the thumnails if they were previously present.</p>
   *
   * @param amazonS3Client the S3 client
   * @param s3Bucket the S3 bucket
   * @param webResourceMetaInfo the provided web resource metadata for conversion
   * @param resourceUrl the resource url that the web resource metadata is based upon
   * @param md5Hex the md5 hash of the resource url
   * @return the converted class
   * @throws MediaExtractionException if a conversion failed
   * @throws IOException if the thumbnail target names generation fails
   */
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
      //Close immediately, the contained files are not required.
      thumbnail.close();
      thumbnails.add(thumbnail);
    }
    return thumbnails;
  }

  /**
   * It writes all the required information to metis core of a dataset that has just been
   * re-processed.
   *
   * @param datasetId the dataset id of the finished dataset re-processing
   * @param startDate the start date of the re-processing
   * @param endDate the end date of the re-processing
   * @param basicConfiguration the configuration class that contains required properties
   */
  public static void updateMetisCoreWorkflowExecutions(String datasetId, Date startDate,
      Date endDate, BasicConfiguration basicConfiguration) {
    // TODO: 21-5-19 Enable methods when ready
//    createReindexWorkflowExecutions(datasetId, startDate, endDate, basicConfiguration);
//    setInvalidFlagToPlugins(datasetId, basicConfiguration);
  }

  private static void createReindexWorkflowExecutions(String datasetId, Date startDate,
      Date endDate, BasicConfiguration basicConfiguration) {
    final AbstractExecutablePlugin lastExecutionToBeBasedOn = basicConfiguration
        .getMetisCoreMongoDao().getWorkflowExecutionDao()
        .getLastFinishedWorkflowExecutionPluginByDatasetIdAndPluginType(datasetId,
            Collections.singleton(basicConfiguration.getReprocessBasedOnPluginType()), false);

    //Preview Plugin
    final ReindexToPreviewPluginMetadata reindexToPreviewPluginMetadata = new ReindexToPreviewPluginMetadata();
    reindexToPreviewPluginMetadata
        .setRevisionNamePreviousPlugin(lastExecutionToBeBasedOn.getPluginType().name());
    reindexToPreviewPluginMetadata
        .setRevisionTimestampPreviousPlugin(lastExecutionToBeBasedOn.getStartedDate());
    final ReindexToPreviewPlugin reindexToPreviewPlugin = new ReindexToPreviewPlugin(
        reindexToPreviewPluginMetadata);
    reindexToPreviewPlugin
        .setId(new ObjectId().toString() + "-" + reindexToPreviewPlugin.getPluginType().name());
    reindexToPreviewPlugin.setStartedDate(startDate);
    reindexToPreviewPlugin.setFinishedDate(endDate);
    reindexToPreviewPlugin.setPluginStatus(PluginStatus.FINISHED);

    //Publish Plugin
    final ReindexToPublishPluginMetadata reindexToPublishPluginMetadata = new ReindexToPublishPluginMetadata();
    reindexToPublishPluginMetadata
        .setRevisionNamePreviousPlugin(reindexToPreviewPlugin.getPluginType().name());
    reindexToPublishPluginMetadata
        .setRevisionTimestampPreviousPlugin(reindexToPreviewPlugin.getStartedDate());
    final ReindexToPublishPlugin reindexToPublishPlugin = new ReindexToPublishPlugin(
        reindexToPublishPluginMetadata);
    reindexToPublishPlugin
        .setId(new ObjectId().toString() + "-" + reindexToPublishPlugin.getPluginType().name());
    reindexToPublishPlugin.setStartedDate(startDate);
    reindexToPublishPlugin.setFinishedDate(endDate);
    reindexToPublishPlugin.setPluginStatus(PluginStatus.FINISHED);

    final Dataset dataset = basicConfiguration.getMetisCoreMongoDao().getDataset(datasetId);
    final ArrayList<AbstractMetisPlugin> abstractMetisPlugins = new ArrayList<>();
    abstractMetisPlugins.add(reindexToPreviewPlugin);
    abstractMetisPlugins.add(reindexToPublishPlugin);
    final WorkflowExecution workflowExecution = new WorkflowExecution(dataset, abstractMetisPlugins,
        0);
    workflowExecution.setWorkflowStatus(WorkflowStatus.FINISHED);
    workflowExecution.setCreatedDate(startDate);
    workflowExecution.setStartedDate(startDate);
    workflowExecution.setUpdatedDate(endDate);
    workflowExecution.setFinishedDate(endDate);
    basicConfiguration.getMetisCoreMongoDao().getWorkflowExecutionDao().create(workflowExecution);
  }

  private static void setInvalidFlagToPlugins(String datasetId,
      BasicConfiguration basicConfiguration) {
    final List<ExecutablePluginType> invalidatePluginTypes = basicConfiguration
        .getInvalidatePluginTypes();
    final WorkflowExecutionDao workflowExecutionDao = basicConfiguration.getMetisCoreMongoDao()
        .getWorkflowExecutionDao();
    final List<AbstractExecutablePlugin> deprecatedPlugins = invalidatePluginTypes.stream()
        .map(executablePluginType -> workflowExecutionDao
            .getLastFinishedWorkflowExecutionPluginByDatasetIdAndPluginType(datasetId,
                Collections.singleton(executablePluginType), false)).collect(Collectors.toList());

    deprecatedPlugins.stream().map(abstractExecutablePlugin -> {
      final WorkflowExecution workflowExecution = workflowExecutionDao
          .getByExternalTaskId(Long.parseLong(abstractExecutablePlugin.getExternalTaskId()));
      final Optional<AbstractMetisPlugin> metisPluginWithType = workflowExecution
          .getMetisPluginWithType(abstractExecutablePlugin.getPluginType());
      metisPluginWithType.ifPresent(
          abstractMetisPlugin -> ((AbstractExecutablePlugin) abstractMetisPlugin)
              .setDataStatus(DataStatus.DEPRECATED));
      return workflowExecution;
    }).forEach(workflowExecutionDao::update);
  }
}
