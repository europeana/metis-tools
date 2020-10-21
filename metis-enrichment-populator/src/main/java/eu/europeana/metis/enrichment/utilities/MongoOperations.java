package eu.europeana.metis.enrichment.utilities;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import eu.europeana.enrichment.api.external.model.LabelInfo;
import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.internal.model.TimespanEnrichmentEntity;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import eu.europeana.normalization.util.XmlException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class MongoOperations {

  private final MongoDatabase mongoDatabase;
  private final EnrichmentDao enrichmentDao;

  public MongoOperations(MongoClient mongoClient, String mongoDatabaseName) {
    this.mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
    this.enrichmentDao = new EnrichmentDao(mongoClient, mongoDatabaseName);
  }

  public void replaceSemiumWithEntities(EnrichmentDao enrichmentDao,
      List<EnrichmentTerm> enrichmentTerms) {
    // TODO: 20/10/2020 Change to update the children
    for (EnrichmentTerm enrichmentTerm : enrichmentTerms) {
      final String semiumId = enrichmentTerm.getEnrichmentEntity().getOwlSameAs().stream()
          .filter(owlSameAs -> owlSameAs.contains("semium")).findFirst().orElseThrow();

      final List<Pair<String, String>> fieldNamesAndValues = new ArrayList<>();
      fieldNamesAndValues.add(new ImmutablePair<>("parent", semiumId));
      final List<EnrichmentTerm> children = enrichmentDao
          .getAllEnrichmentTermsByFields(fieldNamesAndValues);
      final String parentAbout = enrichmentTerm.getEnrichmentEntity().getAbout();
      children.forEach(child -> {
        child.setParent(parentAbout);
        final HashMap<String, List<String>> isPartOf = new HashMap<>();
        isPartOf.put("def", List.of(parentAbout));
        ((TimespanEnrichmentEntity) (child.getEnrichmentEntity())).setIsPartOf(isPartOf);
        enrichmentDao.saveEnrichmentTerm(child);
      });
      //      enrichmentDao.deleteEnrichmentTerms(EntityType.TIMESPAN, List.of(semiumId));
      //      enrichmentDao.deleteEnrichmentTerms(EntityType.TIMESPAN,
      //          List.of(enrichmentTerm.getEnrichmentEntity().getAbout()));
//      enrichmentDao.saveEnrichmentTerm(enrichmentTerm);
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

  public void updateTimespanEntitiesFromFile(String timespanFileWithXmls)
      throws IOException, XmlException {
    final List<EnrichmentTerm> enrichmentTermTimespans = EnrichmentTermUtils
        .getTimespansFromDocument(timespanFileWithXmls);
    replaceSemiumWithEntities(enrichmentDao, enrichmentTermTimespans);
  }

  public void updateEnrichmentTermsFields() {
    final List<EnrichmentTerm> allEnrichmentTermsByFields = enrichmentDao
        .getAllEnrichmentTermsByFields(Collections.emptyList());
    updateFields(allEnrichmentTermsByFields);
    saveAll(enrichmentDao, allEnrichmentTermsByFields);
  }

  private void updateFields(List<EnrichmentTerm> enrichmentTerms) {
    for (EnrichmentTerm enrichmentTerm : enrichmentTerms) {
      //LabelInfos update
      final List<LabelInfo> labelInfoList = EnrichmentTermUtils
          .createLabelInfoList(enrichmentTerm.getEnrichmentEntity());
      enrichmentTerm.setLabelInfos(labelInfoList);
      //      //owlSameAs update
      //      enrichmentTerm.getEnrichmentEntity()
      //          .setOwlSameAs(EnrichmentTermUtils.createOwlSameAsList(enrichmentTerm));
      //      enrichmentTerm.setOwlSameAs(null);
      //      //Remove codeUri from enrichmentTerm
      //      final String entityAbout = enrichmentTerm.getEnrichmentEntity().getAbout();
      //      if (StringUtils.isBlank(entityAbout)) {
      //        enrichmentTerm.getEnrichmentEntity().setAbout(enrichmentTerm.getCodeUri());
      //      }
      //      enrichmentTerm.setCodeUri(null);

    }
  }
}
