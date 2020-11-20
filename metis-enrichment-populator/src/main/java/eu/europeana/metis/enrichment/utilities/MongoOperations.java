package eu.europeana.metis.enrichment.utilities;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.service.dao.EnrichmentDao;
import java.util.List;

public class MongoOperations {

  private final MongoDatabase mongoDatabase;
  private final EnrichmentDao enrichmentDao;

  public MongoOperations(MongoClient mongoClient, String mongoDatabaseName) {
    this.mongoDatabase = mongoClient.getDatabase(mongoDatabaseName);
    this.enrichmentDao = new EnrichmentDao(mongoClient, mongoDatabaseName);
  }

//  public void updateIsPartOfParentField(EntityType entityType) {
//    final List<Pair<String, String>> fieldNamesAndValues = new ArrayList<>();
//    fieldNamesAndValues.add(new ImmutablePair<>("entityType", entityType.name()));
//    final HashMap<String, List<Pair<String, String>>> fieldNameMap = new HashMap<>();
//    fieldNameMap.put(null, fieldNamesAndValues);
//    final List<EnrichmentTerm> allEnrichmentTermsByFields = enrichmentDao
//        .getAllEnrichmentTermsByFields(fieldNameMap);
//    for (EnrichmentTerm enrichmentTerm : allEnrichmentTermsByFields) {
//      if (entityType.equals(EntityType.PLACE)) {
//        final String isPartOfParent = ((PlaceEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
//            .getIsPartOfParent();
//        ((PlaceEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
//            .setIsPartOf(isPartOfParent);
//        ((PlaceEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
//            .setIsPartOfParent(null);
//      } else {
//        final String isPartOfParent = ((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
//            .getIsPartOfParent();
//        ((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
//            .setIsPartOf(isPartOfParent);
//        ((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
//            .setIsPartOfParent(null);
//      }
//      enrichmentDao.saveEnrichmentTerm(enrichmentTerm);
//    }
//
//  }

  //  @Deprecated
  //  public void updateIsPartOfField(EntityType entityType) {
  //
  //    final List<Pair<String, String>> fieldNamesAndValues = new ArrayList<>();
  //    fieldNamesAndValues.add(new ImmutablePair<>("entityType", entityType.name()));
  //    final HashMap<String, List<Pair<String, String>>> fieldNameMap = new HashMap<>();
  //    fieldNameMap.put(null, fieldNamesAndValues);
  //    final List<EnrichmentTerm> allEnrichmentTermsByFields = enrichmentDao
  //        .getAllEnrichmentTermsByFields(fieldNameMap);
  //    for (EnrichmentTerm enrichmentTerm : allEnrichmentTermsByFields) {
  //      final String parent = enrichmentTerm.getParent();
  //      if (StringUtils.isNotBlank(parent)) {
  //
  //        final Map<String, List<String>> isPartOfMap;
  //        if (entityType.equals(EntityType.PLACE)) {
  //          isPartOfMap = ((PlaceEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
  //              .getIsPartOf();
  //        } else {
  //          isPartOfMap = ((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
  //              .getIsPartOf();
  //        }
  //        if (MapUtils.isNotEmpty(isPartOfMap)) {
  //          final List<String> isPartOfList = isPartOfMap.get("def");
  //          if (CollectionUtils.isNotEmpty(isPartOfList)) {
  //            if (isPartOfList.get(0).equals(parent)) {
  //              enrichmentTerm.setParent(null);
  //              if (entityType.equals(EntityType.PLACE)) {
  //                ((PlaceEnrichmentEntity) enrichmentTerm.getEnrichmentEntity()).setIsPartOf(null);
  //                ((PlaceEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
  //                    .setIsPartOfParent(parent);
  //              } else {
  //                ((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity()).setIsPartOf(null);
  //                ((TimespanEnrichmentEntity) enrichmentTerm.getEnrichmentEntity())
  //                    .setIsPartOfParent(parent);
  //              }
  //
  //            }
  //          }
  //        }
  //      }
  //      enrichmentDao.saveEnrichmentTerm(enrichmentTerm);
  //    }
  //  }

  //  @Deprecated
  //  public void getStatisticsForParentField() {
  //    final List<Pair<String, String>> fieldNamesAndValues = new ArrayList<>();
  //    fieldNamesAndValues.add(new ImmutablePair<>("entityType", "PLACE"));
  //    final HashMap<String, List<Pair<String, String>>> fieldNameMap = new HashMap<>();
  //    fieldNameMap.put(null, fieldNamesAndValues);
  //    final List<EnrichmentTerm> allEnrichmentTermsByFields = enrichmentDao
  //        .getAllEnrichmentTermsByFields(fieldNameMap);
  //    int parentsCounter = 0;
  //    int isPartOfDefHigherThanOne = 0;
  //    int isPartOfDefHigherThanTwo = 0;
  //    int parentChainNotPartOfIsPartOf = 0;
  //    int parentNotEqualsToIsPartOf = 0;
  //    for (EnrichmentTerm enrichmentTerm : allEnrichmentTermsByFields) {
  //      final String parent = enrichmentTerm.getParent();
  //      if (StringUtils.isNotBlank(parent)) {
  //        parentsCounter++;
  //        final Map<String, List<String>> isPartOfMap = ((PlaceEnrichmentEntity) enrichmentTerm
  //            .getEnrichmentEntity()).getIsPartOf();
  //        if (MapUtils.isNotEmpty(isPartOfMap)) {
  //          final List<String> isPartOfList = isPartOfMap.get("def");
  //          if (CollectionUtils.isNotEmpty(isPartOfList)) {
  //            if (isPartOfList.size() > 1) {
  //              isPartOfDefHigherThanOne++;
  //              //              String currentParent = parent;
  //              //              List<String> collectedParentAbout = new ArrayList<>();
  //              //              do {
  //              //                final List<Pair<String, String>> fnav = new ArrayList<>();
  //              //                fnav.add(new ImmutablePair<>("enrichmentEntity.about", currentParent));
  //              //                final HashMap<String, List<Pair<String, String>>> fnm = new HashMap<>();
  //              //                fnm.put(null, fnav);
  //              //                final List<EnrichmentTerm> children = enrichmentDao
  //              //                    .getAllEnrichmentTermsByFields(fnm);
  //              //                collectedParentAbout.add(currentParent);
  //              //                currentParent = children.get(0).getParent();
  //              //              } while (StringUtils.isNotBlank(currentParent));
  //              //
  //              //              for (String parentAbout : collectedParentAbout) {
  //              //                if (!isPartOfList.contains(parentAbout)) {
  //              //                  parentChainNotPartOfIsPartOf++;
  //              //                }
  //              //              }
  //            }
  //            if (isPartOfList.size() > 2) {
  //              isPartOfDefHigherThanTwo++;
  //            }
  //            if (!isPartOfList.get(0).equals(parent)) {
  //              parentNotEqualsToIsPartOf++;
  //            }
  //          }
  //        }
  //      }
  //    }
  //
  //    System.out.printf("Total Parents: %s%n", parentsCounter);
  //    System.out.printf("Total Higher than one isPartOfDef: %s%n", isPartOfDefHigherThanOne);
  //    System.out.printf("Total Higher than two isPartOfDef: %s%n", isPartOfDefHigherThanTwo);
  //    System.out
  //        .printf("Total Parent chain not contained in isPartOf: %s%n", parentChainNotPartOfIsPartOf);
  //    System.out.printf("Total Not Equals ParentIsPartOf: %s%n", parentNotEqualsToIsPartOf);
  //  }

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
}
