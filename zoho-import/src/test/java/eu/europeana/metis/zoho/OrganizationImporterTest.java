package eu.europeana.metis.zoho;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.Date;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import eu.europeana.corelib.definitions.edm.entity.Organization;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;
import eu.europeana.enrichment.service.zoho.model.ZohoOrganizationAdapter;
import eu.europeana.metis.zoho.model.UpdateOperation;

//@Ignore
public class OrganizationImporterTest extends BaseImporterTest{

  @Test
  public void testUpdateInEntityAPI() throws Exception{
    OrganizationImporter importer = new OrganizationImporter();
    importer.init();
    importer.updateInEntityApi(BNF_ENTITY_ID_URI);
    
    assertTrue(importer.getEntitySolrImporter().exists(BNF_ENTITY_ID_URI));
  }
  
  @Test
  public void testUpdateInMetis() throws Exception{
    OrganizationImporter importer = new OrganizationImporter();
    importer.init();
    
    List<ZohoOrganization> orgs = importer.getOneOrganizationAsList(BNF_ENTITY_ID_URI);
    UpdateOperation operation = new UpdateOperation(orgs.get(0));
    importer.convertToEdmOrganization(operation);
    // enrich with Wikidata
    importer.enrichWithWikidata(operation);
    importer.updateInMetis(operation);
    Organization org = importer.entityService.getOrganizationById(BNF_ENTITY_ID_URI);
    assertNotNull(org.getAddress().getVcardHasGeo());
    
    //assertTrue(importer.getEntitySolrImporter().exists(ENTITY_ID_URL));
  }
}
