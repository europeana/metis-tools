package eu.europeana.metis.enrichment.utilities;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import eu.europeana.enrichment.api.external.model.LabelInfo;
import eu.europeana.enrichment.internal.model.AbstractEnrichmentEntity;
import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.internal.model.TimespanEnrichmentEntity;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import eu.europeana.enrichment.utils.EntityType;
import eu.europeana.normalization.util.XmlException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class MongoOperations {

  private final MongoDatabase mongoDatabase;
  private final EnrichmentDao enrichmentDao;

  public MongoOperations(MongoClient mongoClient, String mongoDatabaseName) {
    this.mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
    this.enrichmentDao = new EnrichmentDao(mongoClient, mongoDatabaseName);
  }

  @Deprecated
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

  public void updateTimespanEntities(EnrichmentDao enrichmentDao,
      List<EnrichmentTerm> providedEnrichmentTerms) {
    final List<EnrichmentTerm> storedEnrichmentTerms = new ArrayList<>();
    for (EnrichmentTerm providedEnrichmentTerm : providedEnrichmentTerms) {
      final List<Pair<String, String>> fieldNamesAndValues = new ArrayList<>();
      fieldNamesAndValues.add(new ImmutablePair<>("enrichmentEntity.about",
          providedEnrichmentTerm.getEnrichmentEntity().getAbout()));
      storedEnrichmentTerms
          .add(enrichmentDao.getAllEnrichmentTermsByFields(fieldNamesAndValues).get(0));
    }
    final Map<String, String> matchingAbouts = findMatchingAbouts(storedEnrichmentTerms,
        providedEnrichmentTerms);

    //Remove Entities
    enrichmentDao.deleteEnrichmentTerms(EntityType.TIMESPAN,
        storedEnrichmentTerms.stream().map(EnrichmentTerm::getEnrichmentEntity)
            .map(AbstractEnrichmentEntity::getAbout).collect(Collectors.toList()));

    //Add new entities
    saveAll(enrichmentDao, providedEnrichmentTerms);

    List<EnrichmentTerm> childrenEntities = new ArrayList<>();
    for (EnrichmentTerm providedEnrichmentTerm : providedEnrichmentTerms) {

      final List<Pair<String, String>> parentFieldValue = new ArrayList<>();
      parentFieldValue.add(new ImmutablePair<>("parent",
          providedEnrichmentTerm.getEnrichmentEntity().getAbout()));
      final List<EnrichmentTerm> children = enrichmentDao
          .getAllEnrichmentTermsByFields(parentFieldValue);
      //Update children
      children.forEach(child -> {
        final String newParentAbout = matchingAbouts.get(child.getParent());
        child.setParent(newParentAbout);
        final HashMap<String, List<String>> isPartOf = new HashMap<>();
        isPartOf.put("def", List.of(newParentAbout));
        ((TimespanEnrichmentEntity) (child.getEnrichmentEntity())).setIsPartOf(isPartOf);
        childrenEntities.add(child);
      });
    }

    saveAll(enrichmentDao, childrenEntities);
  }

  private Map<String, String> findMatchingAbouts(List<EnrichmentTerm> storedEnrichmentTerms,
      List<EnrichmentTerm> providedEnrichmentTerms) {
    final HashMap<String, String> aboutsMap = new HashMap<>();
    for (EnrichmentTerm storedEnrichment : storedEnrichmentTerms) {
      for (EnrichmentTerm providedEnrichment : providedEnrichmentTerms) {
        if (sameTimespans(storedEnrichment, providedEnrichment)) {
          aboutsMap.put(storedEnrichment.getEnrichmentEntity().getAbout(),
              providedEnrichment.getEnrichmentEntity().getAbout());
        }
      }
    }
    return aboutsMap;
  }

  private boolean sameTimespans(EnrichmentTerm storedTimespan, EnrichmentTerm providedTimespan) {
    return ((TimespanEnrichmentEntity) storedTimespan.getEnrichmentEntity()).getBegin().get("def")
        .get(0).equalsIgnoreCase(
            ((TimespanEnrichmentEntity) providedTimespan.getEnrichmentEntity()).getBegin()
                .get("def").get(0));
  }

  public void saveAll(EnrichmentDao enrichmentDao, List<EnrichmentTerm> enrichmentTerms) {
    enrichmentTerms.forEach(enrichmentDao::saveEnrichmentTerm);
  }

  @Deprecated
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
    updateTimespanEntities(enrichmentDao, enrichmentTermTimespans);
  }

  @Deprecated
  public void updateEnrichmentTermsFields() {
    final List<EnrichmentTerm> allEnrichmentTermsByFields = enrichmentDao
        .getAllEnrichmentTermsByFields(Collections.emptyList());
    updateFields(allEnrichmentTermsByFields);
    saveAll(enrichmentDao, allEnrichmentTermsByFields);
  }

  @Deprecated
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
