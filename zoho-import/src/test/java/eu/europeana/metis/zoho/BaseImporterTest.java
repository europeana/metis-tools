package eu.europeana.metis.zoho;

public class BaseImporterTest {

  private static final String BASE_DATA_EUROPEANA_EU_ORGANIZATION = "http://data.europeana.eu/organization/";
public static final String BNF_ENTITY_ID = "1482250000002112001";
  public static final String SSA_ENTITY_ID = "1482250000004513401";

  public static final String XML_FILE = BNF_ENTITY_ID + ".xml";
  public static final String BNF_ENTITY_URI = BASE_DATA_EUROPEANA_EU_ORGANIZATION + BNF_ENTITY_ID;
  public static final String SAS_ENTITY_URI = BASE_DATA_EUROPEANA_EU_ORGANIZATION + SSA_ENTITY_ID;

  public static final String PROPERTIES_FILE = "/zoho_import.properties";
  
}
