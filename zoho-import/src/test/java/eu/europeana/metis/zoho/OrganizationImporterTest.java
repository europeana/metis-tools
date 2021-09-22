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
    List<Record> org = importer.getOneOrganizationAsList(BNF_ENTITY_URI);
    UpdateOperation operation = new UpdateOperation(org.get(0));
    importer.convertToEnrichmentOrganization(operation);
    importer.enrichWithWikidata(operation);
    importer.updateInMetis(operation);
    Optional<OrganizationEnrichmentEntity> organisation = importer.getEnrichmentService().getOrganizationByUri(BNF_ENTITY_URI);
    assertTrue(organisation.isPresent());
    System.out.println("");
    // delete the organization when needed (individual update will always update the organization)
//    importer.getEnrichmentService().deleteOrganization(BNF_ENTITY_URI);
  }

  @Test
  public void testRunIndividualReimport() throws Exception {
    // will run the importer for criteria roles
    OrganizationImporter.main(new String[]{"individual", SAS_ENTITY_URI});
    Optional<OrganizationEnrichmentEntity> org = importer.getEnrichmentService().getOrganizationByUri(SAS_ENTITY_URI);

    assertTrue(org.isPresent());
    assertEquals(SAS_ENTITY_URI, org.get().getAbout());
    assertEquals(SSA_ENTITY_ID, org.get().getDcIdentifier().get(Constants.UNDEFINED_LANGUAGE_KEY).get(0));

    // delete the organization when needed (individual update will always update the organization)
//    importer.getEnrichmentService().deleteOrganization(SAS_ENTITY_URI);
  }

  @Test
  public void testRunIndividualReimportAlbertina() throws Exception {
    // will run the importer for criteria roles
    OrganizationImporter.main(new String[]{"individual", ALBERTINA_ENTITY_URI});
    Optional<OrganizationEnrichmentEntity> org = importer.getEnrichmentService().getOrganizationByUri(ALBERTINA_ENTITY_URI);

    assertTrue(org.isPresent());
    assertEquals(ALBERTINA_ENTITY_URI, org.get().getAbout());
    assertEquals(ALBERTINA_ENTITY_ID, org.get().getDcIdentifier().get(Constants.UNDEFINED_LANGUAGE_KEY).get(0));

    // delete the organization when needed (individual update will always update the organization)
//    importer.getEnrichmentService().deleteOrganization(SAS_ENTITY_URI);
  }
}
