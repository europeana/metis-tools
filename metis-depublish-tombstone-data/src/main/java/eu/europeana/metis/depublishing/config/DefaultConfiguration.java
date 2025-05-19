package eu.europeana.metis.depublishing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.entity.client.exception.EntityClientException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.depublishing.utilities.IndexUtilities;
import eu.europeana.metis.depublishing.utilities.PostProcessUtilities;
import eu.europeana.metis.depublishing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import java.net.URISyntaxException;
import java.util.Date;
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
  private final ThrowingBiConsumer<RDF, Configuration> rdfIndexer;
  private final ThrowingQuadConsumer<String, Date, Date, Configuration> afterReprocessProcessor;


  public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws URISyntaxException, TrustStoreConfigurationException, IndexingException, NormalizationConfigurationException, EntityClientException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::removeTombstone;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;
  }

  @Override
  public ThrowingBiFunction<FullBeanImpl, Configuration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  @Override
  public ThrowingBiConsumer<RDF, Configuration> getRdfIndexer() {
    return rdfIndexer;
  }

  @Override
  public ThrowingQuadConsumer<String, Date, Date, Configuration> getAfterReprocessProcessor() {
    return afterReprocessProcessor;
  }

  @Override
  public RDF processRDF(RDF rdf) {
    return rdf;
  }

}
