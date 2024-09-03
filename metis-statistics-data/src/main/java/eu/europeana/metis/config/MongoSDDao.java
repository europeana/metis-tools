package eu.europeana.metis.config;

import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.Morphia;
import dev.morphia.mapping.MapperOptions;
import dev.morphia.mapping.NamingStrategy;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filter;
import dev.morphia.query.filters.Filters;
import eu.europeana.metis.model.Historical;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

import static eu.europeana.metis.network.ExternalRequestUtil.retryableExternalRequestForNetworkExceptions;

/**
 * Data access object for the Statistics Dashboard Mongo.
 */
public class MongoSDDao {

    private final Datastore datastore;

    /**
     * Constructor.
     *
     * @param mongoClient       The mongo client.
     * @param mongoDatabaseName The name of the database in the Mongo.
     * @param createIndexes     The flag that initiates the database indexes
     */
    public MongoSDDao(MongoClient mongoClient, String mongoDatabaseName, boolean createIndexes) {
        final MapperOptions mapperOptions = MapperOptions.builder()
                .collectionNaming(NamingStrategy.identity()).build();
        this.datastore = Morphia.createDatastore(mongoClient, mongoDatabaseName, mapperOptions);
        this.datastore.getMapper().map(Historical.class);
        if (createIndexes) {
            datastore.ensureIndexes();
        }
    }


    /**
     * Saves a list of historical data to the database.
     *
     * @param records the list of historical records.
     */
    public void saveHistoricalRecord(List<Historical> records) {
        records.forEach(record -> record.setId(new ObjectId()));
        retryableExternalRequestForNetworkExceptions(() -> datastore.save(records));

    }

    /**
     * Returns all existing values of countries from Historical data collection
     *
     * @return All existing values of countries from Historical data collection
     */
    public List<String> getAllCountryValuesHistoricalCollection() {
        ArrayList<String> countries = new ArrayList<>();
        DistinctIterable<String> docs = retryableExternalRequestForNetworkExceptions(() -> datastore
                .getCollection(Historical.class).distinct("country", String.class));
        docs.forEach(countries::add);
        return countries;
    }


    /**
     * Returns all historical data of a given country
     *
     * @param country - The country to get the historical data from
     * @return The historical data of a given country
     */
    public List<Historical> getAllHistoricalOfCountry(String country) {
        ArrayList<Historical> queryResult = new ArrayList<>();
        Filter filter = Filters.eq("country", country);
        Query<Historical> result = retryableExternalRequestForNetworkExceptions(() ->
                datastore.find(Historical.class).filter(filter));
        result.forEach(queryResult::add);
        return queryResult;
    }

}
