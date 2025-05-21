package eu.europeana.metis.depublishing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.depublishing.utilities.IndexUtilities;

import eu.europeana.metis.depublishing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.net.URISyntaxException;


/**
 * Extra configuration class that is part of {@link Configuration}.
 * <p>This class is meant to be modifiable and different per re-processing operation.
 * It contains 3 functional interfaces that should be initialized properly and they are triggered internally during the
 * re-processing.</p>
 */
public class DefaultConfiguration extends Configuration {

  private final ThrowingBiFunction<FullBeanImpl, Configuration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, FullBeanImpl, Configuration> rdfIndexer;

  public DefaultConfiguration(PropertiesHolder propertiesHolderExtension)
      throws URISyntaxException, TrustStoreConfigurationException, IndexingException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::removeTombstone;
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
    return rdf;
  }

}
