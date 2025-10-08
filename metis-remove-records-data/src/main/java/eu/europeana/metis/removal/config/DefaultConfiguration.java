package eu.europeana.metis.removal.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.removal.utilities.IndexUtilities;
import eu.europeana.metis.removal.utilities.ProcessUtilities;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extra configuration class that is part of {@link Configuration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly and they are triggered internally during the
 * re-processing.</p>
 */
public class DefaultConfiguration extends Configuration {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConfiguration.class);

  private final ThrowingBiFunction<FullBeanImpl, Configuration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, FullBeanImpl, Configuration> rdfIndexer;
  private final RdfConversionUtils rdfConversionUtils = new RdfConversionUtils();

  public DefaultConfiguration(PropertiesHolder propertiesHolderExtension) throws
      URISyntaxException, TrustStoreConfigurationException, IndexingException, IOException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::removeRecord;
  }

  @Override
  public ThrowingBiFunction<FullBeanImpl, Configuration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  @Override
  public ThrowingTriConsumer<RDF, FullBeanImpl, Configuration> getRdfIndexer() {
    return rdfIndexer;
  }

  @Override
  public RDF processRDF(RDF rdf) {
    LOGGER.debug("DONE");
    return rdf;
  }
}
