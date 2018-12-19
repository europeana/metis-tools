package eu.europeana.metis.zoho.python;

import static org.junit.Assert.assertTrue;
import java.io.File;
import org.junit.Ignore;
import org.junit.Test;
import eu.europeana.metis.zoho.OrganizationImporter;
import eu.europeana.metis.zoho.python.SolrDocGeneratorPy;

@Ignore
public class SolrDocGeneratorPyTest {

  @Test
  public void testGenerateSolrDoc() throws Exception{
    OrganizationImporter importer = new OrganizationImporter();
    importer.loadProperties(OrganizationImporter.PROPERTIES_FILE);
    
    SolrDocGeneratorPy generator = new SolrDocGeneratorPy(
        importer.getProperty(OrganizationImporter.PROP_PYTHON),
        importer.getProperty(OrganizationImporter.PROP_PYTHON_PATH),
        importer.getProperty(OrganizationImporter.PROP_PYTHON_SCRIPT),
        importer.getProperty(OrganizationImporter.PROP_PYTHON_WORKDIR));
    File solrDoc = generator.generateSolrDoc("http://data.europeana.eu/organization/1482250000002112001");
    assertTrue(solrDoc.exists());
  }
}
