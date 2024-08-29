package eu.europeana.metis;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.config.MongoSDDao;
import eu.europeana.metis.config.ConfigurationPropertiesHolder;
import eu.europeana.metis.config.DataAccessConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ScriptsRunner implements CommandLineRunner {

    private static final String COMMA_DELIMITER = ",";

    private final ConfigurationPropertiesHolder propertiesHolder;
    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptsRunner.class);

    public ScriptsRunner(ConfigurationPropertiesHolder propertiesHolder) {
        this.propertiesHolder = propertiesHolder;
    }

    @Override
    public void run(String... args) throws DataAccessConfigException {

        final MongoClientProvider<DataAccessConfigException> mongoSDClientProvider = new MongoClientProvider<>(propertiesHolder.getMongoSDProperties());

        try(final MongoClient mongoSDClient = mongoSDClientProvider.createMongoClient()){

            MongoSDDao mongoSDDao = new MongoSDDao(mongoSDClient, propertiesHolder.getMongoSDDatabase(), true);



        }

    }

    private static List<List<String>> readCsvFile(String fileName) {
        List<List<String>> result = new ArrayList<>();
        LOGGER.info("Started reading document");
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(COMMA_DELIMITER);
                result.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOGGER.info("Finished reading document");
        return result;
    }

}
