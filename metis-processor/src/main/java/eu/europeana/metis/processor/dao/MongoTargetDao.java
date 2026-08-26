package eu.europeana.metis.processor.dao;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.SocketSettings;
import dev.morphia.Datastore;
import dev.morphia.DatastoreImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.properties.mongo.MongoTargetProperties;
import eu.europeana.metis.processor.utilities.FullbeanUtil;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class MongoTargetDao {

  public static final String ABOUT = "about";
  private static final int DEFAULT_PAGE_SIZE = 10;
  public static int PAGE_SIZE = DEFAULT_PAGE_SIZE;
  private final MongoTargetProperties mongoTargetProperties;
  private final MongoClient mongoClient;
  private final Datastore metisTargetDatastore;
  private final FullbeanUtil fullbeanUtil;

  public MongoTargetDao(MongoTargetProperties mongoTargetProperties) throws DataAccessConfigException {
    this.mongoTargetProperties = mongoTargetProperties;
    this.mongoClient = initializeMongoClient();
    this.metisTargetDatastore = initializeDatastore();
    this.fullbeanUtil = new FullbeanUtil();

  }

  private MongoClient initializeMongoClient() throws DataAccessConfigException {
    // Configure SocketSettings with a 16 MB buffer for send and receive
    SocketSettings socketSettings = SocketSettings.builder()
                                                  .receiveBufferSize(16 * 1024 * 1024) // 16 MB buffer for receiving
                                                  .sendBufferSize(16 * 1024 * 1024)    // 16 MB buffer for sending
                                                  .connectTimeout(30, TimeUnit.SECONDS) // Connection timeout
                                                  .readTimeout(30, TimeUnit.SECONDS)    // Read timeout
                                                  .build();
    MongoClientSettings.Builder settings = MongoClientSettings.builder()
                                                              .applyToSocketSettings(
                                                                  builder -> builder.applySettings(socketSettings))
                                                              .compressorList(
                                                                  Arrays.asList(MongoCompressor.createSnappyCompressor(),
                                                                      MongoCompressor.createZlibCompressor(),
                                                                      MongoCompressor.createZstdCompressor()));
    return new MongoClientProvider<>(mongoTargetProperties.getMongoTargetProperties(), settings).createMongoClient();
  }

  private Datastore initializeDatastore() {
    RecordDao recordDao = new RecordDao(mongoClient, mongoTargetProperties.getMongoTargetDatabase());
    DatastoreImpl datastore = (DatastoreImpl) recordDao.getDatastore();
    datastore.applyIndexes();
    return datastore;
  }
}
