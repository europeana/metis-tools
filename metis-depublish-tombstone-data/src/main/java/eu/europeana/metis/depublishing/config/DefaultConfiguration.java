package eu.europeana.metis.depublishing.config;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.enrichment.api.external.impl.ClientEntityResolver;
import eu.europeana.enrichment.api.external.model.EnrichmentBase;
import eu.europeana.enrichment.api.internal.AggregationFieldType;
import eu.europeana.enrichment.api.internal.EntityResolver;
import eu.europeana.enrichment.api.internal.FieldType;
import eu.europeana.enrichment.api.internal.FieldValue;
import eu.europeana.enrichment.api.internal.ReferenceTermContext;
import eu.europeana.enrichment.api.internal.SearchTermContext;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.utils.EntityMergeEngine;
import eu.europeana.entity.client.EntityApiClient;
import eu.europeana.entity.client.config.EntityClientConfiguration;
import eu.europeana.entity.client.exception.EntityClientException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.depublishing.exception.ProcessingException;
import eu.europeana.metis.depublishing.utilities.IndexUtilities;
import eu.europeana.metis.depublishing.utilities.PostProcessUtilities;
import eu.europeana.metis.depublishing.utilities.ProcessUtilities;
import eu.europeana.metis.schema.convert.RdfConversionUtils;
import eu.europeana.metis.schema.convert.SerializationException;
import eu.europeana.metis.schema.jibx.AboutType;
import eu.europeana.metis.schema.jibx.Aggregation;
import eu.europeana.metis.schema.jibx.Organization;
import eu.europeana.metis.schema.jibx.RDF;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
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
  private final ThrowingTriConsumer<RDF, Boolean, Configuration> rdfIndexer;
  private final ThrowingQuadConsumer<String, Date, Date, Configuration> afterReprocessProcessor;


  public DefaultConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws URISyntaxException, TrustStoreConfigurationException, IndexingException, NormalizationConfigurationException, EntityClientException {
    super(propertiesHolderExtension);

    this.fullBeanProcessor = ProcessUtilities::processFullBean;
    this.rdfIndexer = IndexUtilities::indexRecord;
    this.afterReprocessProcessor = PostProcessUtilities::postProcess;
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
  public ThrowingQuadConsumer<String, Date, Date, Configuration> getAfterReprocessProcessor() {
    return afterReprocessProcessor;
  }

  @Override
  public RDF processRDF(RDF rdf) {
    return rdf;
  }

}
