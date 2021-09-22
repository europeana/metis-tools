package eu.europeana.metis.utils;

/**
 * Constants for Organisation Importer Class
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public final class Constants {

    // Arguments
    public static final String IMPORT_FULL = "full";
    public static final String IMPORT_INCREMENTAL = "incremental";
    public static final String IMPORT_DATE = "date";
    public static final String IMPORT_INDIVIDUAL = "individual";

    // Converter Utils
    public static final String UNDEFINED_LANGUAGE_KEY = "def";
    public static final int LANGUAGE_CODE_LENGTH = 5;
    public static final int SAME_AS_CODE_LENGTH = 2;


    // TODO move to eu.europeana.metis.zoho.ZohoConstants
    public static final String URL_ORGANIZATION_PREFFIX = "http://data.europeana.eu/organization/";
    public static final String ZOHO_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String ZOHO_OWNER_CRITERIA = "Owner.name";
    public static final String ZOHO_OWNER_FIELD = "Owner";

    // wikidata
    public static final String WIKIDATA_ORGANIZATION_XSL_FILE = "/wkd2org.xsl";
    public static final String SPARQL = "https://query.wikidata.org/sparql";
    public static final String WIKIDATA_BASE_URL = "http://www.wikidata.org/entity/Q";
    public static final int SIZE = 1048576;
    public static final String RDF_ABOUT = "rdf_about";

    // Operations
    public static final String ACTION_CREATE = "create";
    public static final String ACTION_UPDATE = "update";
    public static final String ACTION_DELETE = "delete";

    // Others
    public static final String ADDRESS_ABOUT = "#address";


}