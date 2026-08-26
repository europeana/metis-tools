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
import eu.europeana.metis.schema.jibx.ProvidedCHOType;
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
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
  private static final String[] datasetsToNormalise = new String[] {
      "179","08711","428","331","260","497","498","302","569","619","2064947",
      "2064934","2064910","2064912","2064913","2064907","2055737","2064901","131","13","2064950","2064945","2064951","2064925",
      "2064944","2064949","2064929","2064903","1001","2064931","69","3","2064920","2064902","12","2064909","2064948","2064927",
      "2064940","2064908","2064923","2064905","2048210","1367","2064904","2064930","2064916","1231","2064915","2064942","2020107",
      "1251","1327","92058","92056","92068","92057","15","9200394","38","1055","194","2020128","2020127","284","92053","221","222",
      "223","5","92","940","258","2020109","707","934","706","785","139","615","719","921","15409","215","328","922","15405","214",
      "641","255","916","920","327","394","217","938","919","1089","155","216","438","393","1119","915","574","392","917","169",
      "204","884","1129","898","1395","1397","1394","1392","1393","1400","1169","1401","1399","1396","1063","1398","1168","1402",
      "1229","08535","990","504","672","2048424","495","713","368","358","372","362","2048437","2048441","492","315","2048425",
      "2048426","572","2022712","304","879","353","340","593","297","299","877","303","307","541","1423","881","355","1248",
      "10501","298","640","598","763","764","744","815","848","816","425","2021651","2021657","203","629","855","1415","231",
      "09315","228","229","809","630","637","318","230","09317","233","2048221","739","2059219","2059205","2059204","0940431",
      "235","237","238","0940420","0940429","0940439","92040","2020708","421","467","2024907","652","653","657","659","589","654",
      "655","656","658","660","661","2048374","1150","889","1247","2048128","1440","15515","490","2021004","2021003","2021001",
      "2021006","2021005","9200133","9200352","932","1297","1312","1371","1372","1459","865","1313","1373","996","1315","1450",
      "1370","1375","1302","1298","1299","1314","869","1303","1377","1376","1285","1508","1374","1449","1490","1382","1507","858",
      "857","859","1080","1504","856","1565","9200449","9200408","9200140","9200365","9200324","9200373","9200364","9200317",
      "9200395","92097","9200119","9200182","9200382","9200173","9200385","9200167","9200118","92080","92075","9200111","9200110",
      "2022039","2022076","2022054","2022065","2022077","2022078","2022079","2022038","2022001","2022002","2022068","2022042",
      "2022058","2022023","2022024","2022025","2022037","2022080","2022082","2022083","2022062","2022084","2022052","2022064",
      "2022043","2022044","2058208","2058201","2058207","2058206","2022089","2022091","2022093","2022094","2022095","2022096",
      "180","2048603","2048621","2048620","2048614","9200498","9200516","9200517","794","9200518","124","9200519","9200521",
      "9200522","780","2059510","08904","08804","08803","2020710","1","101","102","14","103","104","90402","07931","07932",
      "9200211","08574","1391","808","91625","91617","91672","76","916118","154","916108","916105","916124","916121","91670",
      "134","916123","916109","916106","91616","91668","91627","91666","91619","91608","916122","91674","91658","91676","1465",
      "91624","900","1045","133","77","916119","958","1042","410","1044","91609","916120","91698","91650","91652","75","72",
      "1351","91653","91641","91682","91654","91643","91639","916113","91640","1039","1177","347","916114","91659","916110",
      "91669","71","90","52","91607","91699","91673","91694","323","91647","91644","916117","346","412","901","91683","916116",
      "91680","348","91631","144","602","1040","91663","91642","73","91646","565","132","601","944","91662","91685","916101",
      "91657","916115","153","535","916107","91691","91656","1489","74","847","916100","91687","156","91695","91648","91655",
      "91645","91688","91690","91651","91660","91689","91671","91684","91665","411","916104","91697","916112","1349","1350",
      "1538","91693","1095","1097" };

  /**
   * Instantiates a new Default configuration.
   *
   * @param propertiesHolderExtension the properties holder extension
   * @throws DereferenceException the dereference exception
   * @throws EnrichmentException the enrichment exception
   * @throws URISyntaxException the uri syntax exception
   * @throws TrustStoreConfigurationException the trust store configuration exception
   * @throws IndexingException the indexing exception
   * @throws NormalizationConfigurationException the normalization configuration exception
   * @throws EntityClientException the entity client exception
   */
  public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws DereferenceException, EnrichmentException, URISyntaxException, TrustStoreConfigurationException, IndexingException, NormalizationConfigurationException, EntityClientException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;
    initializeAdditionalElements(propertiesHolderExtension);
  }

  /**
   * Read file to string string.
   *
   * @param file the file
   * @return the string
   * @throws IOException the io exception
   */
  public static String readFileToString(String file) throws IOException {
    ClassLoader classLoader = DefaultConfiguration.class.getClassLoader();
    try (InputStream inputStream = classLoader.getResourceAsStream(file)) {
      if (inputStream == null) {
        throw new IOException("Failed reading file " + file);
      }
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    }
  }

  /**
   * Rename to main for tests.
   *
   * @param args the args
   * @throws IndexingException the indexing exception
   * @throws DereferenceException the dereference exception
   * @throws NormalizationConfigurationException the normalization configuration exception
   * @throws TrustStoreConfigurationException the trust store configuration exception
   * @throws EnrichmentException the enrichment exception
   * @throws URISyntaxException the uri syntax exception
   * @throws SerializationException the serialization exception
   * @throws ProcessingException the processing exception
   * @throws EntityClientException the entity client exception
   * @throws IOException the io exception
   */
  public static void renameToMainForTests(String[] args)
      throws IndexingException, DereferenceException, NormalizationConfigurationException,
      TrustStoreConfigurationException, EnrichmentException, URISyntaxException,
      SerializationException, ProcessingException, EntityClientException, IOException {
    DefaultConfiguration defaultConfiguration = new DefaultConfiguration(new PropertiesHolderExtension(
        "application.properties"));

    processAnXmlFile(defaultConfiguration);

    processARecordFromMongoSource(defaultConfiguration);

  }

  private static void processARecordFromMongoSource(DefaultConfiguration defaultConfiguration)
      throws ProcessingException, SerializationException {
    List<FullBeanImpl> fullBeanList = Stream
        .of(
            "/867/https___hispana_mcu_es_lod_oai_prensahistorica_mcu_es_11000468588_ent0",
            "/817/NHMUKXZOOX1935X8X20X138"
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
    if (hasDatasetIdOfRecordToBeNormalised(rdf)) {
      rdf = normalizePIDS(rdf);
      LOGGER.info("Normalisation DONE");
    }
    return rdf;
  }

  /**
   * Is dataset on whitelist boolean.
   *
   * @param datasetId the dataset id
   * @return true when is on whitelist otherwise false
   */
  public static boolean isDatasetOnWhitelist(String datasetId) {
    return Arrays.asList(datasetsToNormalise).contains(datasetId);
  }

  private static boolean hasDatasetIdOfRecordToBeNormalised(RDF rdf) {
    Optional<String> about = rdf.getProvidedCHOList()
                                .stream()
                                .filter(Objects::nonNull)
                                .findFirst()
                                .map(ProvidedCHOType::getAbout);
    if (about.isPresent()) {
      final String[] splitRecordIdentifier = about.get().split("/");
      String datasetId = splitRecordIdentifier[1];
      return isDatasetOnWhitelist(datasetId);
    }
    return false;
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
   *
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
