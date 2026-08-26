package eu.europeana.metis.reprocessing.utilities;

import com.mongodb.MongoWriteException;
import eu.europeana.corelib.edm.utils.EdmUtils;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.entity.client.exception.EntityClientException;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import eu.europeana.indexing.tiers.TierCalculationMode;
import eu.europeana.metis.network.ExternalRequestUtil;
import eu.europeana.metis.reprocessing.config.Configuration;
import eu.europeana.metis.reprocessing.config.DefaultConfiguration;
import eu.europeana.metis.reprocessing.config.PropertiesHolderExtension;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.schema.jibx.AboutType;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains functionality for indexing.
 * <p>Methods in this class will be provided as implementations of functional interfaces for
 * performing the indexing of records</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019 -05-17
 */
public class IndexUtilities {

  private static final Map<Class<?>, String> retryExceptions;
  private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    retryExceptions = new HashMap<>(ExternalRequestUtil.UNMODIFIABLE_MAP_WITH_NETWORK_EXCEPTIONS);
    retryExceptions.put(MongoWriteException.class, "E11000 duplicate key error collection");
  }

  private IndexUtilities() {
  }

  /**
   * Indexes a record using properties from a provided {@link Configuration}
   *
   * @param rdf the source rdf record
   * @param preserveTimestamps should preserve timestamps from source
   * @param configuration the configuration class that contains required properties
   * @throws IndexingException if an exception occurred during indexing
   */
  public static void indexRecord(RDF rdf, Boolean preserveTimestamps, Configuration configuration)
      throws IndexingException {
    try {
      //The indexer pool shouldn't be closed here, therefore it's not initialized in a
      // try-with-resources block
      final IndexerPool indexerPool = configuration.getDestinationIndexerPool();
      ExternalRequestUtil.retryableExternalRequest(() -> {
        //Timestamps should be preserved, Redirects calculation disabled
        final Date recordDate = null;
        final List<String> datasetIdsForRedirection = null;
        final boolean performRedirects = false;
        final TierCalculationMode tierCalculationMode = configuration.getTierCalculationMode();
        final IndexingProperties indexingProperties = new IndexingProperties(recordDate, preserveTimestamps,
            datasetIdsForRedirection, performRedirects, tierCalculationMode);
        final String rdfAbout = rdf.getProvidedCHOList()
                                   .stream()
                                   .filter(Objects::nonNull)
                                   .map(AboutType::getAbout)
                                   .findFirst()
                                   .orElse("");
        LOGGER.info("Indexing record {}", rdfAbout);
        indexerPool.indexRdf(rdf, indexingProperties);

//        if (configuration.isDepublicationEnabled()) {
//          final String datasetId = getDatasetIdOfRecordToBePurged(rdf);
//          depublishRecord(rdf, datasetId, rdfAbout, indexerPool);
//        }
        return null;
      }, retryExceptions);
    } catch (Exception e) {
      throw new RecordRelatedIndexingException("A Runtime Exception occurred while indexing record", e);
    }
  }

  public static void renameToMainToTest(String[] args)
  //public static void main(String[] args)
      throws IndexingException, DereferenceException, NormalizationConfigurationException,
      TrustStoreConfigurationException, EntityClientException, EnrichmentException, URISyntaxException,
      ProcessingException {

    DefaultConfiguration defaultConfiguration = new DefaultConfiguration(new PropertiesHolderExtension(
        "application.properties"));

    List<FullBeanImpl> fullBeanList = Stream
        .of(
            "/1469/21_15123_LLYgrJMp" // webresource bug
            /*"/318/marc_nli_000042980", //Illegal character code 0xd83c in content text
            "/318/marc_nli_000042981",
            "/318/marc_nli_000042984",
            "/318/marc_nli_000042985",
            "/318/marc_nli_000042987",
            "/318/marc_nli_000042994",
            "/318/marc_nli_000042995",
            "/318/marc_nli_000042998",
            "/318/marc_nli_000042999",
            "/318/marc_nli_000043002",
            "/318/marc_nli_000043006",
            "/318/marc_nli_000043007",
            "/318/marc_nli_000043008",
            "/318/marc_nli_000043009",
            "/318/marc_nli_000043022",
            "/318/marc_nli_000043042",
            "/318/marc_nli_000043044",
            "/318/marc_nli_000043045",
            "/318/marc_nli_000043046",
            "/318/marc_nli_000043048",
            "/318/marc_nli_000043049",
            "/318/marc_nli_000043050",
            "/318/marc_nli_000043052",
            "/318/marc_nli_000043053",
            "/318/marc_nli_000043057",
            "/318/marc_nli_000043059",
            "/318/marc_nli_000043061",
            "/318/marc_nli_000043064",
            "/318/marc_nli_000043066",
            "/318/marc_nli_000043075",
            "/318/marc_nli_000043078",
            "/318/marc_nli_000043086",
            "/318/marc_nli_000043087",
            "/318/marc_nli_000043088",
            "/318/marc_nli_000043090",
            "/318/marc_nli_000043092",
            "/318/marc_nli_000043094",
            "/318/marc_nli_000043100",
            "/318/marc_nli_000043102",
            "/318/marc_nli_000043103",
            "/318/marc_nli_000043106",
            "/318/marc_nli_000043110",
            "/318/marc_nli_000043112",
            "/318/marc_nli_000043113",
            "/318/marc_nli_000043123",
            "/318/marc_nli_000043131",
            "/318/marc_nli_000043132",
            "/318/marc_nli_000043135",
            "/318/marc_nli_000043137",
            "/318/marc_nli_000043142",
            "/318/marc_nli_000043144",
            "/318/marc_nli_000043145",
            "/318/marc_nli_000043146",
            "/318/marc_nli_000043147",
            "/318/marc_nli_000043152",
            "/318/marc_nli_000043154",
            "/318/marc_nli_000043155",
            "/318/marc_nli_000043156",
            "/318/marc_nli_000043163",
            "/318/marc_nli_000043164",
            "/318/marc_nli_000043165",
            "/318/marc_nli_000043183",
            "/318/marc_nli_000043184",
            "/318/marc_nli_000043186",
            "/318/marc_nli_000043189",
            "/318/marc_nli_000043192",
            "/318/marc_nli_000043196",
            "/318/marc_nli_000043200",
            "/318/marc_nli_000043201",
            "/318/marc_nli_000043202",
            "/318/marc_nli_000043203",
            "/318/marc_nli_000043206",
            "/318/marc_nli_000043207",
            "/318/marc_nli_000043208",
            "/318/marc_nli_000043209",
            "/318/marc_nli_000043210",
            "/318/marc_nli_000043211",
            "/318/marc_nli_000043213",
            "/318/marc_nli_000043216",
            "/318/marc_nli_000043217",
            "/318/marc_nli_000043221",
            "/318/marc_nli_000043222",
            "/318/marc_nli_000043226",
            "/318/marc_nli_000043231",
            "/318/marc_nli_000043239",
            "/318/marc_nli_000043247",
            "/318/marc_nli_000043248",
            "/318/marc_nli_000043249", //Illegal character code 0xd83c in content text
            "/318/marc_nli_002926270", //Illegal character code 0xd83c in content text
            "/318/marc_nli_002926554",
            "/318/marc_nli_002926562",
            "/318/marc_nli_002926571",
            "/318/marc_nli_002926575",
            "/318/marc_nli_002926576",
            "/318/marc_nli_002926973",
            "/318/marc_nli_002927018",
            "/318/marc_nli_003012771",
            "/318/marc_nli_003013370",
            "/318/marc_nli_003013372",
            "/318/marc_nli_003013710",
            "/318/marc_nli_003013789",
            "/318/marc_nli_003014366",
            "/318/marc_nli_003014435",
            "/318/marc_nli_003014962",
            "/318/marc_nli_003014968",
            "/318/marc_nli_003014970",
            "/318/marc_nli_003014975",
            "/318/marc_nli_003015204",
            "/318/marc_nli_003015230",
            "/318/marc_nli_003015254",
            "/318/marc_nli_003015785",
            "/318/marc_nli_003016115",
            "/318/marc_nli_003016653",
            "/318/marc_nli_003016655",
            "/318/marc_nli_003016668",
            "/318/marc_nli_003016687",
            "/318/marc_nli_003016698",
            "/318/marc_nli_003016711",
            "/318/marc_nli_003016765",
            "/318/marc_nli_003017087",
            "/318/marc_nli_003017180",
            "/318/marc_nli_003017199",
            "/318/marc_nli_003017629",
            "/318/marc_nli_003018045",
            "/318/marc_nli_003018376",
            "/318/marc_nli_003018382",
            "/318/marc_nli_003018387",
            "/318/marc_nli_003018396",
            "/318/marc_nli_003018404",
            "/318/marc_nli_003018408",
            "/318/marc_nli_003018411",
            "/318/marc_nli_003018417",
            "/318/marc_nli_003022951",
            "/318/marc_nli_003022985",
            "/318/marc_nli_003024240",
            "/318/marc_nli_003024488",
            "/318/marc_nli_003024499",
            "/318/marc_nli_003024529",
            "/318/marc_nli_003024849",
            "/318/marc_nli_003024933"*/
        )
        .map(item -> defaultConfiguration.getMongoSourceMongoDao().getRecordsFromList(List.of(item)))
        .flatMap(List::stream)
        .toList();

    for (FullBeanImpl fb : fullBeanList) {

      RDF rdf = defaultConfiguration.getFullBeanProcessor().apply(fb, defaultConfiguration);
      indexRecord(rdf, true, defaultConfiguration);
    }
  }
}
