package eu.europeana.metis.zoho;

import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class OrganizationImporterTest extends BaseImporterTest{

  @Test
  public void testReimportEntity() throws Exception{
    OrganizationImporter importer = new OrganizationImporter();
    importer.init();
    importer.updateInEntityApi(ENTITY_ID_URL);
    
    assertTrue(importer.getEntitySolrImporter().exists(ENTITY_ID_URL));
  }
}
