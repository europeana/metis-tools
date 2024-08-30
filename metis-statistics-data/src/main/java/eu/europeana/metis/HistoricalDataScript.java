package eu.europeana.metis;

import com.mongodb.client.MongoClient;
import eu.europeana.metis.model.Historical;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.config.MongoSDDao;
import eu.europeana.metis.config.ConfigurationPropertiesHolder;
import eu.europeana.metis.config.DataAccessConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class HistoricalDataScript implements CommandLineRunner {

    private static final String COMMA_DELIMITER = ",";

    private final ConfigurationPropertiesHolder propertiesHolder;
    private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalDataScript.class);

    public HistoricalDataScript(ConfigurationPropertiesHolder propertiesHolder) {
        this.propertiesHolder = propertiesHolder;
    }

    @Override
    public void run(String... args) throws DataAccessConfigException {

        File folder = new File("src/main/resources/historicalData");
        File[] dataFiles = Objects.requireNonNull(folder.listFiles());

        final MongoClientProvider<DataAccessConfigException> mongoSDClientProvider = new MongoClientProvider<>(propertiesHolder.getMongoSDProperties());

        try(final MongoClient mongoSDClient = mongoSDClientProvider.createMongoClient()){
            MongoSDDao mongoSDDao = new MongoSDDao(mongoSDClient, propertiesHolder.getMongoSDDatabase(), true);

            for(File file : dataFiles) {
                List<List<String>> fileContent = readCsvFile(file.getAbsolutePath());
                writeHistoricalData(fileContent, mongoSDDao);
            }

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

    private static void writeHistoricalData(List<List<String>> targetData, MongoSDDao mongoSDDao){
        final List<Historical> results = new ArrayList<>();
        List<String> firstRow = targetData.getFirst();
        LocalDateTime calculationDate = LocalDateTime.parse(firstRow.getFirst());
        LOGGER.info("Started writing data into database");
        for(int i = 1; i < targetData.size(); i++){
            List<String> row = targetData.get(i);
            LOGGER.info("Started writing data of country {} into database", row.get(0));
            Historical data = new Historical(row.get(0), null,
                    Integer.parseInt(row.get(2)), Integer.parseInt(row.get(1)), calculationDate);
            results.add(data);
        }

        mongoSDDao.saveHistoricalRecord(results);
        LOGGER.info("Finished writing data.");

    }

}
