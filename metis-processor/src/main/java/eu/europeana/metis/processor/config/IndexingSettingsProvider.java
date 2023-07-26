package eu.europeana.metis.processor.config;

import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import eu.europeana.metis.processor.properties.general.SolrZookeeperTargetProperties;
import eu.europeana.metis.processor.properties.mongo.MongoTargetProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

public class IndexingSettingsProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MongoTargetProperties mongoTargetProperties;

    private final SolrZookeeperTargetProperties solrZookeeperTargetProperties;

    public IndexingSettingsProvider(MongoTargetProperties mongoTargetProperties, SolrZookeeperTargetProperties solrZookeeperTargetProperties) {
        this.mongoTargetProperties = mongoTargetProperties;
        this.solrZookeeperTargetProperties = solrZookeeperTargetProperties;
    }

    public IndexingSettings getIndexingSettings() throws IndexingException, URISyntaxException {
        IndexingSettings indexingSettings = new IndexingSettings();
        prepareMongoSettings(indexingSettings);
        prepareSolrSettings(indexingSettings);
        prepareZookeeperSettings(indexingSettings);

        return indexingSettings;
    }

    private void prepareMongoSettings(IndexingSettings indexingSettings) throws IndexingException {
        for (int i = 0; i < mongoTargetProperties.getMongoTargetHosts().length; i++) {
            if (mongoTargetProperties.getMongoTargetHosts().length == mongoTargetProperties.getMongoTargetPorts().length) {
                indexingSettings.addMongoHost(
                        new InetSocketAddress(mongoTargetProperties.getMongoTargetHosts()[i],
                                mongoTargetProperties.getMongoTargetPorts()[i]));
            } else { // Same port for all
                indexingSettings.addMongoHost(
                        new InetSocketAddress(mongoTargetProperties.getMongoTargetHosts()[i],
                                mongoTargetProperties.getMongoTargetPorts()[0]));
            }
        }
        indexingSettings.setMongoDatabaseName(mongoTargetProperties.getMongoTargetDatabase());
        if (StringUtils.isEmpty(mongoTargetProperties.getMongoTargetAuthenticationDatabase()) || StringUtils
                .isEmpty(mongoTargetProperties.getMongoTargetUsername()) || StringUtils
                .isEmpty(mongoTargetProperties.getMongoTargetPassword())) {
            LOGGER.info("Mongo credentials not provided");
        } else {
            indexingSettings.setMongoCredentials(mongoTargetProperties.getMongoTargetUsername(),
                    mongoTargetProperties.getMongoTargetPassword(),
                    mongoTargetProperties.getMongoTargetAuthenticationDatabase());
        }

        if (mongoTargetProperties.isMongoTargetEnableSsl()) {
            indexingSettings.setMongoEnableSsl();
        }
    }

    private void prepareSolrSettings(IndexingSettings indexingSettings)
            throws URISyntaxException, SetupRelatedIndexingException {
        for (String instance : solrZookeeperTargetProperties.getSolrTargetHosts()) {
            indexingSettings
                    .addSolrHost(new URI(instance + solrZookeeperTargetProperties.getZookeeperTargetDefaultCollection()));
        }
    }

    private void prepareZookeeperSettings(IndexingSettings indexingSettings)
            throws SetupRelatedIndexingException {
        for (int i = 0; i < solrZookeeperTargetProperties.getZookeeperTargetHosts().length; i++) {
            if (solrZookeeperTargetProperties.getZookeeperTargetHosts().length
                    == solrZookeeperTargetProperties.getZookeeperTargetPorts().length) {
                indexingSettings.addZookeeperHost(
                        new InetSocketAddress(solrZookeeperTargetProperties.getZookeeperTargetHosts()[i],
                                solrZookeeperTargetProperties.getZookeeperTargetPorts()[i]));
            } else { // Same port for all
                indexingSettings.addZookeeperHost(
                        new InetSocketAddress(solrZookeeperTargetProperties.getZookeeperTargetHosts()[i],
                                solrZookeeperTargetProperties.getZookeeperTargetPorts()[0]));
            }
        }
        indexingSettings.setZookeeperChroot(solrZookeeperTargetProperties.getZookeeperTargetChroot());
        indexingSettings
                .setZookeeperDefaultCollection(solrZookeeperTargetProperties.getZookeeperTargetDefaultCollection());
    }
}
