package eu.europeana.metis.entity;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides tests for add, commit and delete methods for Entity API Solr importer.
 * 
 * @author GrafR
 *
 */
public class EntityApiSolrImporterTest {
  
  private static File xmlFile;
  public static final String ENTITY_ID = "1482250000002112001";
  public static final String XML_FILE = ENTITY_ID + ".xml";
  public static final String ENTITY_ID_URL = "http://data.europeana.eu/organization/" + ENTITY_ID; 
  public static final String PROPERTIES_FILE = "/zoho_import.properties";
  private static EntityApiSolrImporter entityApiSolrImporter;
  private static String docsFolder = "";
  
  static final Logger LOGGER = LoggerFactory.getLogger(EntityApiSolrImporterTest.class);

  @Before
  public void setUp() throws IOException, URISyntaxException {
    Properties appProps = null;
    String solrUrl = "";
    
    try {
      appProps = loadProperties(PROPERTIES_FILE);
      solrUrl = appProps.getProperty("entity.importer.solr.url");
      docsFolder = appProps.getProperty("entity.api.solr.docs.folder");
      LOGGER.info("using Solr URL: " + solrUrl);
      LOGGER.info("using documents folder: " + docsFolder);
      
    } catch (Exception e) {
      LOGGER.error("Cannot initialize importer!", e);
      return;// the job cannot be run
    }
    
    entityApiSolrImporter = new EntityApiSolrImporter(solrUrl);
    xmlFile = getOrganizationXmlExampleFile(XML_FILE);
  }

  protected Properties loadProperties(String propertiesFile)
      throws URISyntaxException, IOException, FileNotFoundException {
    Properties appProps = new Properties();
    appProps.load( getClass().getResourceAsStream(propertiesFile));
    return appProps;
  }

  @Test
  public void testAdd() throws SolrServerException, IOException {
    entityApiSolrImporter.add(xmlFile, true);
    assertTrue(entityApiSolrImporter.exists(ENTITY_ID_URL));
  }

  @Test
  public void testDelete() throws SolrServerException, IOException {
    entityApiSolrImporter.delete(ENTITY_ID_URL, true);
    assertFalse(entityApiSolrImporter.exists(ENTITY_ID_URL));
  }

  /**
   * This method loads XML example file for given path for Organization testing
   * @param filePath
   * @return XML file with organization entity
   * @throws IOException
   * @throws URISyntaxException
   */
  private File getOrganizationXmlExampleFile(String filePath) throws IOException, URISyntaxException {
    String path = docsFolder + "/" + filePath;
    File xmlOrganizationExampleFile = new File(path);
      return xmlOrganizationExampleFile;
  }  
}