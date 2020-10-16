package eu.europeana.metis.enrichment.utilities;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import eu.europeana.enrichment.api.external.model.LabelInfo;
import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.internal.model.TimespanEnrichmentEntity;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import eu.europeana.enrichment.utils.EntityType;
import eu.europeana.normalization.util.XmlException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MongoOperations {

  private final MongoDatabase mongoDatabase;
  private final EnrichmentDao enrichmentDao;

  public MongoOperations(MongoClient mongoClient, String mongoDatabaseName) {
    this.mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
    this.enrichmentDao = new EnrichmentDao(mongoClient, mongoDatabaseName);
  }

  public void replaceSemiumWithEntities(EnrichmentDao enrichmentDao,
      List<EnrichmentTerm> enrichmentTerms) {
    for (EnrichmentTerm enrichmentTerm : enrichmentTerms) {
      final String semiumId = Arrays
          .stream(((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity()).getOwlSameAs())
          .filter(owlSameAs -> owlSameAs.contains("semium")).findFirst().orElseThrow();
      enrichmentDao.deleteEnrichmentTerms(EntityType.TIMESPAN, List.of(semiumId));
      enrichmentDao.saveEnrichmentTerm(enrichmentTerm);
    }
  }

  public void saveAll(EnrichmentDao enrichmentDao, List<EnrichmentTerm> enrichmentTerms) {
    enrichmentTerms.forEach(enrichmentDao::saveEnrichmentTerm);
  }

  public void updateClassNameInAddress() {
    final BasicDBObject command = new BasicDBObject();
    command.put("eval",
        "db.getCollection('EnrichmentTerm').find({'enrichmentEntity.address.className':'eu.europeana.corelib.solr.entity.AddressImpl'})\n"
            + "  .forEach(function (doc) {\n"
            + "    doc.enrichmentEntity.address.className = 'eu.europeana.enrichment.internal.model.Address';\n"
            + "    db.getCollection('EnrichmentTerm').save(doc);\n" + "});");
    mongoDatabase.runCommand(command);
  }

  public void updateTimespanEntitiesFromFile(String timespanFileWithXmls) throws IOException, XmlException {
    final List<EnrichmentTerm> enrichmentTermTimespans = EnrichmentTermUtils
        .getTimespansFromDocument(timespanFileWithXmls);
    replaceSemiumWithEntities(enrichmentDao, enrichmentTermTimespans);
  }

  public void updateEnrichmentTermsLabelInfos() {
    final List<EnrichmentTerm> allEnrichmentTermsByFields = enrichmentDao
        .getAllEnrichmentTermsByFields(Collections.emptyList());
    updateLabelInfos(allEnrichmentTermsByFields);
    saveAll(enrichmentDao, allEnrichmentTermsByFields);
  }

  private void updateLabelInfos(List<EnrichmentTerm> enrichmentTerms) {
    for (EnrichmentTerm enrichmentTerm : enrichmentTerms) {
      final List<LabelInfo> labelInfoList = EnrichmentTermUtils
          .createLabelInfoList(enrichmentTerm.getEnrichmentEntity());
      enrichmentTerm.setLabelInfos(labelInfoList);
    }
  }
}
