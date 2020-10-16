package eu.europeana.metis.creator.utilities;

import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.internal.model.TimespanEnrichmentEntity;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import eu.europeana.enrichment.utils.EntityType;
import java.util.Arrays;
import java.util.List;

public class MongoPopulator {

  private MongoPopulator() {
  }

  public static void replaceSemiumWithEntities(EnrichmentDao enrichmentDao,
      List<EnrichmentTerm> enrichmentTerms) {
    for (EnrichmentTerm enrichmentTerm : enrichmentTerms) {
      final String semiumId = Arrays
          .stream(((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity()).getOwlSameAs())
          .filter(owlSameAs -> owlSameAs.contains("semium")).findFirst().orElseThrow();
      enrichmentDao.deleteEnrichmentTerms(EntityType.TIMESPAN, List.of(semiumId));
      enrichmentDao.saveEnrichmentTerm(enrichmentTerm);
    }
  }

}
