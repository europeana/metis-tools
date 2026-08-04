package eu.europeana.metis.reprocessing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver.OperationMode;
import eu.europeana.enrichment.api.internal.EntityResolver;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.entity.client.EntityApiClient;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.exception.EntityClientException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.utilities.IndexUtilities;
import eu.europeana.metis.reprocessing.utilities.PostProcessUtilities;
import eu.europeana.metis.reprocessing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.convert.SerializationException;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.NormalizerStep;
import eu.europeana.normalization.model.NormalizationResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extra configuration class that is part of {@link Configuration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly, and they are triggered internally during the
 * re-processing.</p>
 */
public class DefaultConfiguration extends Configuration {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConfiguration.class);

  private final ThrowingBiFunction<FullBeanImpl, Configuration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, Boolean, Configuration> rdfIndexer;
  private final ThrowingQuadConsumer<String, Instant, Instant, Configuration> afterReprocessProcessor;
  private final RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();
  private final Normalizer normalizer = new NormalizerFactory().getNormalizer(NormalizerStep.NORMALIZE_PIDS);
  private ClientEntityResolver entityResolver;


  public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException, URISyntaxException, TrustStoreConfigurationException, IndexingException, NormalizationConfigurationException, EntityClientException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;
    initializeAdditionalElements(propertiesHolderExtension);
  }

  public static String readFileToString(String file) throws IOException {
    ClassLoader classLoader = DefaultConfiguration.class.getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(file);
    if (inputStream == null) {
      throw new IOException("Failed reading file " + file);
    }
    return new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
  }

  public static void renameToMainForTests(String[] args)
      throws IndexingException, DereferenceException, NormalizationConfigurationException,
      TrustStoreConfigurationException, EnrichmentException, URISyntaxException,
      SerializationException, ProcessingException, EntityClientException, IOException {
    DefaultConfiguration defaultConfiguration = new DefaultConfiguration(new PropertiesHolderExtension(
        "application.properties"));

    processAnXmlFile(defaultConfiguration);

  //  processARecordFromMongoSource(defaultConfiguration);

  }

  private static void processARecordFromMongoSource(DefaultConfiguration defaultConfiguration)
      throws ProcessingException, SerializationException {
    List<FullBeanImpl> fullBeanList = Stream
        .of(
            //i
            "/2020702/raa_fmi_10000100970001",
            //f
            "/2048087/ProvidedCHO_Battersea_Arts_Centre_BAC_9_YT_002_006_002",
            //e
            "/2048128/114145",
            //d
            "/9200579/kyaq8pq9",
            //c
            "/9200359/BibliographicResource_3000123626519",
            "/9200359/BibliographicResource_3000100585617", //with contentTier 4
            "/9200359/BibliographicResource_3000100387622", //with contentTier 1
            //a
            "/9200579/cynwkevu",
            //b
            "/954/Culturalia_fd913fb8_8a14_40c9_94ec_38158f4d4c81",
            //g
            "/1087/https___catalonica_bnc_cat_catalonicahub_lod_oai_arca_bnc_cat_10000296883_ent0",
            //h
            "/164/https___catalonica_bnc_cat_catalonicahub_lod_oai_ddd_uab_cat_100377_ent1"
        )
        .map(item -> defaultConfiguration.getMongoSourceMongoDao().getRecordsFromList(List.of(item)))
        .flatMap(List::stream)
        .toList();

    for (FullBeanImpl fb : fullBeanList) {
      RDF rdf = defaultConfiguration.getFullBeanProcessor().apply(fb, defaultConfiguration);

      LOGGER.info("Before:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
      rdf = defaultConfiguration.normalizePIDS(rdf);
      LOGGER.info("After:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
    }

  }

  private static void processAnXmlFile(DefaultConfiguration defaultConfiguration) throws SerializationException, IOException {
    RDF rdf = defaultConfiguration.rdfConversionUtils.convertStringToRdf(
        readFileToString("unit_test/test_normalization.xml"));
    LOGGER.info("Before:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
    rdf = defaultConfiguration.normalizePIDS(rdf);
    LOGGER.info("After:\r\n{}\r\n", defaultConfiguration.rdfConversionUtils.convertRdfToString(rdf));
  }

  private static String convertToValidURLString(String link) {
    try {
      return URI.create(link).toURL().toString();
    } catch (MalformedURLException e) {
      return null;
    }
  }

  @Override
  public ThrowingBiFunction<FullBeanImpl, Configuration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  @Override
  public ThrowingTriConsumer<RDF, Boolean, Configuration> getRdfIndexer() {
    return rdfIndexer;
  }

  @Override
  public ThrowingQuadConsumer<String, Instant, Instant, Configuration> getAfterReprocessProcessor() {
    return afterReprocessProcessor;
  }

  @Override
  public RDF processRDF(RDF rdf) {
    rdf = normalizePIDS(rdf);
    LOGGER.debug("DONE");
    return rdf;
  }

  private void initializeAdditionalElements(PropertiesHolderExtension propertiesHolderExtension) throws EntityClientException {
    this.entityResolver = (ClientEntityResolver) prepareClientEntityResolver(propertiesHolderExtension);
  }

  private EntityResolver prepareClientEntityResolver(PropertiesHolderExtension propertiesHolderExtension)
      throws EntityClientException {
    //Sanity check
    if (StringUtils.isAnyBlank(propertiesHolderExtension.entityManagementUrl,
        propertiesHolderExtension.entityApiUrl,
        propertiesHolderExtension.entityApiTokenEndpoint,
        propertiesHolderExtension.entityApiGrantParams)) {
      throw new IllegalArgumentException("Requested resolver but configuration is missing");
    }
    final Properties properties = new Properties();
    properties.put("entity.management.url", propertiesHolderExtension.entityManagementUrl);
    properties.put("entity.api.url", propertiesHolderExtension.entityApiUrl);
    properties.put("token_endpoint", propertiesHolderExtension.entityApiTokenEndpoint);
    properties.put("grant_params", propertiesHolderExtension.entityApiGrantParams);

    return new ClientEntityResolver(new EntityApiClient(new EntityClientConfiguration(properties)), OperationMode.NON_CACHED);
  }

  /**
   * Run the europeana PID normalisation to the record
   * @param rdf record
   * @return rdf with normalised PIDs
   */
  RDF normalizePIDS(RDF rdf) {
    LOGGER.info("Normalising PID");
    RDF computedRDF = rdf;
    try {
      final String rdfString = rdfConversionUtils.convertRdfToString(rdf);
      final NormalizationResult result = normalizer.normalize(rdfString);
      computedRDF = rdfConversionUtils.convertStringToRdf(result.getNormalizedRecordInEdmXml());
    } catch (RuntimeException | SerializationException | NormalizationException e) {
      LOGGER.warn("Something went wrong during normalisation", e);
    }
    return computedRDF;
  }
}
