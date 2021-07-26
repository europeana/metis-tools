package eu.europeana.metis.zoho;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.Optional;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.metis.utils.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import eu.europeana.metis.zoho.model.UpdateOperation;

public class OrganizationImporterTest extends BaseImporterTest {

  private static OrganizationImporter importer;

  @BeforeEach
  void setup() throws Exception {
  importer = new OrganizationImporter();
  importer.init();
}

  @Test
  public void testUpdateInMetis() throws Exception{
    List<Record> org = importer.getOneOrganizationAsList(BNF_ENTITY_ID_URI_1);
    UpdateOperation operation = new UpdateOperation(org.get(0));
    importer.convertToEnrichmentOrganization(operation);
    importer.enrichWithWikidata(operation);
    importer.updateInMetis(operation);
    Optional<OrganizationEnrichmentEntity> organisation = importer.getEnrichmentService().getOrganizationByUri(BNF_ENTITY_ID_URI_1);
    assertTrue(organisation.isPresent());

    // delete the organisation
    importer.getEnrichmentService().deleteOrganization(BNF_ENTITY_ID_URI_1);
  }

  @Test
  public void testRunIndividualReimport() throws Exception {
    // will run the importer for criteria roles
    OrganizationImporter.main(new String[]{"individual", BNF_ENTITY_ID_URI_2});
    Optional<OrganizationEnrichmentEntity> org = importer.getEnrichmentService().getOrganizationByUri(BNF_ENTITY_ID_URI_2);

    assertTrue(org.isPresent());
    assertEquals(BNF_ENTITY_ID_URI_2, org.get().getAbout());
    assertEquals(BNF_ENTITY_ID_2, org.get().getDcIdentifier().get(Constants.UNDEFINED_LANGUAGE_KEY).get(0));

    // delete the organisation
    importer.getEnrichmentService().deleteOrganization(BNF_ENTITY_ID_URI_2);
  }

}
