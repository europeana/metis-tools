package eu.europeana.metis.tools.dataset.migration;

import com.opencsv.CSVReader;
import eu.europeana.metis.core.common.Country;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.plugins.OaipmhHarvestPluginMetadata;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles execution of the modes and has methods to convert csv lines to {@link Dataset}
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-14
 */
public class ExecutorManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);

  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDao;
  private int readCounter = 0;
  private int storedCounter = 0;
  private int failedCounter = 0;
  private int deletedCounter = 0;

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDao) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDao = datasetDao;
  }

  public void createMode() {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Start reading the csv file");
    try (CSVReader reader = new CSVReader(new FileReader(propertiesHolder.datasetsCsvPath))) {
      String[] line;
      reader.readNext();//Bypass titles line
      while ((line = reader.readNext()) != null) {
        readCounter++;
        convertLineToDataset(line, datasetDao);
      }
    } catch (IOException e) {
      LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Reading csv file failed ", e);
    }

    LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total lines read: {} ", readCounter);
    LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total datasets stored: {} ", storedCounter);
    LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total datasets failed: {} ", failedCounter);
  }

  public void deleteMode() {
    try (Stream<String> stream = Files.lines(Paths.get(propertiesHolder.datasetIdsPath))) {
      stream.forEach(datasetId -> {
        deletedCounter++;
        datasetDao.deleteByDatasetId(Integer.parseInt(datasetId));
      });
    } catch (IOException e) {
      LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Reading dataset ids file failed ", e);
    }
    LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total datasets deleted: {} ", deletedCounter);
  }

  private void convertLineToDataset(String[] line, DatasetDao datasetDao) {
    Dataset dataset;
    try {
      dataset = extractColumnsFromArray(line);
      Dataset storedDataset = null;
      String datasetIdString = null;
      if (dataset != null) {
        storedDataset = datasetDao.getDatasetByDatasetId(dataset.getDatasetId());
        datasetIdString = Integer.toString(dataset.getDatasetId());
      }

      if (dataset != null && storedDataset == null) {
        datasetDao.create(dataset);
        storedCounter++;
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Dataset with datasetId: {}, created", datasetIdString);
        LOGGER.info(PropertiesHolder.SUCCESSFULL_DATASET_IDS, datasetIdString);
      } else if (storedDataset != null) {
        LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER, "Dataset with datasetId: {}, already exists",
            datasetIdString);
      } else {
        LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER, "Failed to read dataset from csv line.");
        LOGGER.warn(PropertiesHolder.FAILED_CSV_LINES_MARKER, Arrays.toString(line));
        failedCounter++;
      }
    } catch (ParseException e) {
      LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER, "Failed to read dataset from csv line.");
      LOGGER.warn(PropertiesHolder.FAILED_CSV_LINES_MARKER, Arrays.toString(line));
      failedCounter++;
    }
  }

  private Dataset extractColumnsFromArray(String[] line) throws ParseException {
    Dataset dataset = new Dataset();
    dataset.setEcloudDatasetId(String.format("NOT_CREATED_YET-%s", UUID.randomUUID().toString()));
    //Only get the first numeric part of the Columns.NAME field
    Pattern pattern = Pattern.compile("^(\\d+)_.*");
    Matcher matcher = pattern.matcher(line[Columns.NAME.getIndex()].trim());
    if (matcher.find()) {
      String datasetId = matcher.group(1);
      dataset.setDatasetId(Integer.parseInt(datasetId));
    } else {
      return null;
    }
    dataset.setDatasetName(line[Columns.NAME.getIndex()].trim());
    dataset.setOrganizationId(propertiesHolder.organizationId);
    dataset.setOrganizationName(propertiesHolder.organizationName);
    dataset.setCreatedByUserId(propertiesHolder.userId);
    SimpleDateFormat dateFormat = new SimpleDateFormat(propertiesHolder.SUGARCRM_DATE_FORMAT);
    dataset.setCreatedDate(dateFormat.parse(line[Columns.DATE_CREATED.getIndex()].trim()));
    dataset.setCountry(
        Country.getCountryFromIsoCode(line[Columns.DATASET_COUNTRY_CODE.getIndex()].trim()));
    dataset.setDescription(line[Columns.DESCRIPTION.getIndex()]);
    dataset.setNotes(line[Columns.NOTES.getIndex()]);

    if (line[Columns.HARVEST_TYPE.getIndex()].trim().equals("oai_pmh")) {
      OaipmhHarvestPluginMetadata oaipmhHarvestPluginMetadata = new OaipmhHarvestPluginMetadata();
      oaipmhHarvestPluginMetadata.setUrl(line[Columns.HARVEST_URL.getIndex()].trim());
      oaipmhHarvestPluginMetadata
          .setMetadataFormat(line[Columns.METADATA_FORMAT.getIndex()].trim());
      oaipmhHarvestPluginMetadata
          .setSetSpec(line[Columns.SETSPEC.getIndex()].trim().equals("-") ? null
              : line[Columns.SETSPEC.getIndex()].trim());
      oaipmhHarvestPluginMetadata.setMocked(false);
      dataset.setHarvestingMetadata(oaipmhHarvestPluginMetadata);
    }
    return dataset;
  }

}
