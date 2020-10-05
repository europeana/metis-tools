package eu.europeana.metis.mongo.analyzer;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import eu.europeana.metis.mongo.analyzer.utilities.RecordListFields;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reconstructor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Reconstructor.class);

  private final Datastore datastore;
  private final long counterCheckpoint;
  private final String recordAboutToCheck;
  private static final String ABOUT_FIELD = "about";
  private final Path pathWithCorruptedRecords;

  public Reconstructor(Datastore datastore, @Nullable String recordAboutToCheck,
      final long counterCheckpoint, String filePathWithCorruptedRecords) {
    this.datastore = datastore;
    this.counterCheckpoint = counterCheckpoint;
    this.recordAboutToCheck = recordAboutToCheck;
    this.pathWithCorruptedRecords = Paths.get(filePathWithCorruptedRecords);
  }

  public void reconstruct() throws IOException {
    final List<String> recordAbouts;
    if (StringUtils.isBlank(recordAboutToCheck)) {
      //Read all corrupted record abouts
      recordAbouts = Files.readAllLines(pathWithCorruptedRecords);
    } else {
      recordAbouts = List.of(recordAboutToCheck);
    }
    final List<String> fieldListsToCheck = Arrays.stream(RecordListFields.values())
        .map(RecordListFields::getFieldName).collect(Collectors.toList());
    reconstructRecords(datastore, "record", recordAbouts, fieldListsToCheck);
  }

  private void reconstructRecords(Datastore datastore, String collection, List<String> recordAbouts,
      List<String> fieldListsToCheck) {
    final List<Document> aboutsQueries = recordAbouts.stream()
        .map(about -> new Document(ABOUT_FIELD, about)).collect(Collectors.toList());
    int counter = 0;
    for (Document aboutQuery : aboutsQueries) {
      final MongoCollection<Document> mongoCollection = datastore.getDatabase()
          .getCollection(collection);
      final FindIterable<Document> findIterable = mongoCollection.find(aboutQuery);
      reconstructDocuments(fieldListsToCheck, mongoCollection, findIterable);

      counter++;
      if (counter % counterCheckpoint == 0 && LOGGER.isInfoEnabled()) {
        LOGGER.info("Reconstructed {} records from collection {}", counter, collection);
      }
    }
    LOGGER.info("Reconstructed {} records from collection {}", counter, collection);
  }

  private void reconstructDocuments(List<String> fieldListsToCheck,
      MongoCollection<Document> mongoCollection, FindIterable<Document> findIterable) {
    try (MongoCursor<Document> cursor = findIterable.cursor()) {
      while (cursor.hasNext()) {
        final Document document = cursor.next();

        for (String fieldList : fieldListsToCheck) {
          Optional.ofNullable(document.get(fieldList)).map(o -> (List<DBRef>) o)
              .map(this::removeDuplicatesFromList)
              .ifPresent(dbRefsList -> document.put(fieldList, dbRefsList));
        }
        updateDocumentInDb(document, mongoCollection);
      }
    }
  }

  private void updateDocumentInDb(Document document, MongoCollection<Document> mongoCollection) {
    mongoCollection.replaceOne(new Document("_id", document.getObjectId("_id")), document);
  }

  private List<DBRef> removeDuplicatesFromList(List<DBRef> list) {
    return list.stream().distinct().collect(Collectors.toList());
  }

}
