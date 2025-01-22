package eu.europeana.metis.processor.dao;

import static com.mongodb.client.model.Sorts.ascending;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.util.List;
import org.bson.Document;
import org.bson.types.ObjectId;

public class PaginationPOC {

  public static void main(String[] args) {
    // Connect to MongoDB

    try (MongoClient mongoClient = MongoClients.create(
        "mongodb://test:test@test-acc-1-metis.eanadev.org:27027,test-acc-2-metis.eanadev.org:27027,test-acc-3-metis.eanadev.org:27027/?retryWrites=true&loadBalanced=false&replicaSet=new-test-acc-metis&readPreference=primary&serverSelectionTimeoutMS=35000&connectTimeoutMS=30000&authSource=admin&authMechanism=SCRAM-SHA-1")) {
      MongoDatabase database = mongoClient.getDatabase("metis-publish-test");
      FindIterable<Document> collection = database.getCollection("record")
                                                  .find(Filters.regex("about", "^\\/1140/")).sort(ascending("_id"));

      // Pagination parameters
      int pageSize = 25; // Number of documents per page
      ObjectId lastId = null; // Track the last _id from the previous page
      long pages = (long) Math.ceil(
          database.getCollection("record").countDocuments(Filters.regex("about", "^\\/1140/")) * 1.0 / pageSize);
      // Fetch multiple pages
      for (long page = 1; page <= pages; page++) {
        System.out.println("Page " + page + ":");
        List<Document> pageData = fetchPage(collection, lastId, pageSize);

        // Print the current page data
        for (Document doc : pageData) {
          System.out.print(doc.get("_id"));
          System.out.println(" " + doc.get("about"));
        }

        // Update lastId to the _id of the last document in the page
        if (!pageData.isEmpty()) {
          lastId = pageData.get(pageData.size() - 1).getObjectId("_id");
        } else {
          break; // No more documents to paginate
        }
      }
    }
  }

  /**
   * Fetches a page of documents using _id-based pagination.
   *
   * @param collection The MongoDB collection to query.
   * @param lastId The _id of the last document from the previous page (null for the first page).
   * @param pageSize The number of documents to fetch per page.
   * @return A list of documents in the current page.
   */
  public static List<Document> fetchPage(FindIterable<Document> collection, ObjectId lastId, int pageSize) {
    FindIterable<Document> query;

    if (lastId == null) {
      // First page: No _id filter
      query = collection.limit(pageSize);
    } else {
      // Subsequent pages: Use _id > lastId
      query = collection.filter(Filters.gt("_id", lastId)).limit(pageSize).sort(ascending("_id"));
    }

    return query.into(new java.util.ArrayList<>());
  }
}

