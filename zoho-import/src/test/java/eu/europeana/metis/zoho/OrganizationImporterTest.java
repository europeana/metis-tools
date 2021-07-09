package eu.europeana.metis.zoho;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.Optional;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import org.junit.jupiter.api.Test;
import eu.europeana.metis.zoho.model.UpdateOperation;

public class OrganizationImporterTest extends BaseImporterTest{

  @Test
  public void testUpdateInMetis() throws Exception{
    OrganizationImporter importer = new OrganizationImporter();
    importer.init();

    List<Record> org = importer.getOneOrganizationAsList(BNF_ENTITY_ID_URI);
    UpdateOperation operation = new UpdateOperation(org.get(0));
    importer.convertToEnrichmentOrganization(operation);
    importer.enrichWithWikidata(operation);
    importer.updateInMetis(operation);
    Optional<OrganizationEnrichmentEntity> organisation = importer.getEnrichmentService().getOrganizationByUri(BNF_ENTITY_ID_URI);
    assertTrue(organisation.isPresent());
  }
  

  @Test
  public void testRunIndividualReimport() throws Exception{
    //SSA
    String entityId = "http://data.europeana.eu/organization/1482250000004513401";
    OrganizationImporter.main(new String[]{"individual", entityId});

    OrganizationImporter importer = new OrganizationImporter();
    importer.init();
    Optional<OrganizationEnrichmentEntity> org = importer.getEnrichmentService().getOrganizationByUri(entityId);
    assertTrue(org.isPresent());
  }
  
}
