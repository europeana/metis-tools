package eu.europeana.metis.entity;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.europeana.metis.zoho.BaseImporterTest;

/**
 * This class provides tests for add, commit and delete methods for Entity API Solr importer.
 * 
 * @author GrafR
 * @author GordeaS
 *
 */
@Disabled
public class EntityApiSolrImporterTest extends BaseImporterTest{
  
  private static File xmlFile;
  private static EntityApiSolrImporter entityApiSolrImporter;
  private static String docsFolder = "";
  
  static final Logger LOGGER = LoggerFactory.getLogger(EntityApiSolrImporterTest.class);

  @BeforeAll
  public static void setUp() throws IOException, URISyntaxException {
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
  }

  protected static Properties loadProperties(String propertiesFile)
      throws URISyntaxException, IOException, FileNotFoundException {
    Properties appProps = new Properties();
    appProps.load( EntityApiSolrImporterTest.class.getResourceAsStream(propertiesFile));
    return appProps;
  }

  @Test
  public void testAdd() throws SolrServerException, IOException, URISyntaxException {
    xmlFile = getOrganizationXmlExampleFile(XML_FILE);
    entityApiSolrImporter.add(xmlFile, true);
    assertTrue(entityApiSolrImporter.exists(BNF_ENTITY_ID_URI));
  }

  @Test
  public void testDelete() throws SolrServerException, IOException {
    entityApiSolrImporter.delete(BNF_ENTITY_ID_URI, true);
    assertFalse(entityApiSolrImporter.exists(BNF_ENTITY_ID_URI));
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