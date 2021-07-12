package eu.europeana.metis.config;

import com.mongodb.client.MongoClients;
import com.zoho.api.authenticator.store.DBStore;
import com.zoho.api.authenticator.store.FileStore;
import com.zoho.api.authenticator.store.TokenStore;
import eu.europeana.enrichment.service.EnrichmentService;
import eu.europeana.enrichment.service.PersistentEntityResolver;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import eu.europeana.metis.zoho.ZohoAccessClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Configuration for Organisation Importer Class
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public class OrganisationImporterConfig {

    private static final Logger LOGGER = LogManager.getLogger(OrganisationImporterConfig.class);

    public static final String PROP_MONGO_ENRICHMENT_DB_NAME = "mongo.enrichment.database";
    public static final String PROP_MONGO_ENRICHMENT_CONNECTION_URL = "mongo.enrichment.connectionUrl";

    public static final String PROP_ZOHO_ORGANIZATION_SEARCH_CRITERIA_ROLE = "zoho.organization.search.criteria.role";

    public static final String PROP_ZOHO_EMAIL = "zoho.email";
    public static final String PROP_ZOHO_CLIENT_ID = "zoho.client.id";
    public static final String PROP_ZOHO_CLIENT_SECRET = "zoho.client.secret";
    public static final String PROP_ZOHO_GRANT_TOKEN = "zoho.grant.token";
    public static final String PROP_ZOHO_REDIRECT_URL = "zoho.redirect.url";

    public static final String PROP_ZOHO_AUTH_HOST = "zohooauth.host";
    public static final String PROP_ZOHO_AUTH_USER = "zohooauth.user";
    public static final String PROP_ZOHO_AUTH_PASSWORD = "zohooauth.password";
    public static final String PROP_ZOHO_AUTH_PORT = "zohooauth.port";
    public static final String PROP_ZOHO_AUTH_DBNAME = "zohooauth.dbname";

    public static final String PROP_TOKEN_FILE = "token.store.file.path";
    public static final String PROPERTIES_FILE = "/zoho_import.properties";


    Properties appProps;

    public OrganisationImporterConfig() {
        loadProperties(PROPERTIES_FILE);
    }

    private Properties loadProperties(String propertiesFile) {
        try { appProps = new Properties();
            appProps.load(getClass().getResourceAsStream(propertiesFile));
        } catch (IOException e) {
            LOGGER.error("Error loading the properties file {}", PROPERTIES_FILE);
        }
        return appProps;
    }

    /**
     * returns the token store instance of DB persistence
     * @return TokenStore
     */
    public TokenStore getDBTokenStore() {
        return new DBStore(
                getProperty(PROP_ZOHO_AUTH_HOST),
                getProperty(PROP_ZOHO_AUTH_DBNAME),
                getProperty(PROP_ZOHO_AUTH_USER),
                getProperty(PROP_ZOHO_AUTH_PASSWORD),
                getProperty(PROP_ZOHO_AUTH_PORT));
    }

    /**
     * returns the token store instance of DB persistence
     * @return TokenStore
     */
    public FileStore getFileTokenStore() throws Exception {
        return new FileStore(getProperty(PROP_TOKEN_FILE));
    }

    /**
     * returns the instance of ZohoAccessClient
     * @return ZohoAccessClient
     */
    public ZohoAccessClient getZohoAccessClient() throws Exception {
        String zohoGrantToken = getProperty(PROP_ZOHO_GRANT_TOKEN);
        if (zohoGrantToken == null || zohoGrantToken.length() < 6) {
            throw new IllegalArgumentException("zoho.authentication.token is invalid: " + zohoGrantToken);
        }
        LOGGER.info("using zoho authentication token: {} ..." , zohoGrantToken.substring(0, 3));
        return new ZohoAccessClient(
                getTokenStore(),
                getProperty(PROP_ZOHO_EMAIL),
                getProperty(PROP_ZOHO_CLIENT_ID),
                getProperty(PROP_ZOHO_CLIENT_SECRET),
                zohoGrantToken,
                getProperty(PROP_ZOHO_REDIRECT_URL));
    }

    /**
     * Token store will be instantiated.
     * If file location if present, FileStore otherwise DBStore
     *
     * @return
     * @throws Exception
     */
    private TokenStore getTokenStore() throws Exception {
        TokenStore tokenStore = getProperty(PROP_TOKEN_FILE).isEmpty() ? getDBTokenStore() : getFileTokenStore();
        if(tokenStore.getTokens().isEmpty()) {
            throw new IllegalArgumentException("Something went wrong with token store. Please verify the properties for db or file token store");
        }
        return tokenStore;
    }

    /**
     * Initialises the EnrichmentDao
     * @return EnrichmentDao
     */
    public EnrichmentDao getEnrichmentDao() {
        String mongoConnectionUrl = getProperty(PROP_MONGO_ENRICHMENT_CONNECTION_URL);
        String dbName = getProperty(PROP_MONGO_ENRICHMENT_DB_NAME);
        return new EnrichmentDao(MongoClients.create(mongoConnectionUrl), dbName);
    }

    public PersistentEntityResolver getPersistentEntityResolver() {
        return new PersistentEntityResolver(getEnrichmentDao());
    }

    public EnrichmentService getEnrichmentService() {
        return new EnrichmentService(getPersistentEntityResolver());
    }

    public String getSearchFilter() {
        return getProperty(PROP_ZOHO_ORGANIZATION_SEARCH_CRITERIA_ROLE);
    }

    private String getProperty(String propertyName) {
        return appProps.getProperty(propertyName);
    }

}
