package eu.europeana.metis.processor.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.properties.mongo.MongoTargetProperties;
import eu.europeana.metis.processor.utilities.FullbeanUtil;

public class MongoTargetDao {

    private static final int DEFAULT_PAGE_SIZE = 10;
    public static final String ABOUT = "about";
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
        return new MongoClientProvider<>(mongoTargetProperties.getMongoTargetProperties()).createMongoClient();
    }

    private Datastore initializeDatastore() {
        RecordDao recordDao = new RecordDao(mongoClient, mongoTargetProperties.getMongoTargetDatabase());
        return recordDao.getDatastore();
    }
}
