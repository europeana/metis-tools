package eu.europeana.metis.tools.dataset.migration.utilities;

import com.opencsv.CSVReader;
import eu.europeana.metis.core.common.Country;
import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dao.WorkflowDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.Workflow;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.HTTPHarvestPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.OaipmhHarvestPluginMetadata;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles execution of the modes and has methods to convert csv lines to {@link Dataset}
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-14
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);

  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDao;
  private final WorkflowDao workflowDao;
  private int readCounter = 0;
  private int storedCounter = 0;
  private int failedCounter = 0;
  private int deletedCounter = 0;

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDao,
      WorkflowDao workflowDao) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDao = datasetDao;
    this.workflowDao = workflowDao;
  }

  public void createMode() {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Start reading the csv file");
    try (CSVReader reader = new CSVReader(new FileReader(propertiesHolder.datasetsCsvPath))) {
      String[] line;
      reader.readNext();//Bypass titles line
      readCounter++;
      while ((line = reader.readNext()) != null) {
        readCounter++;
        convertLineToDataset(line);
      }
    } catch (IOException e) {
      LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Reading csv file failed ", e);
    }

    LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "Total lines read(including title line): {} ", readCounter);
    LOGGER
        .error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total datasets stored: {} ", storedCounter);
    LOGGER
        .error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total datasets failed: {} ", failedCounter);
  }

  public void deleteMode() {
    try (Stream<String> stream = Files.lines(Paths.get(propertiesHolder.datasetIdsPath))) {
      stream.forEach(datasetId -> {
        deletedCounter++;
        datasetDao.deleteByDatasetId(datasetId);
        workflowDao.deleteWorkflow(datasetId);
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Counter: {}, datasetId: {}",
            deletedCounter, datasetId);
      });
    } catch (IOException e) {
      LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Reading dataset ids file failed ", e);
    }
    LOGGER.error(PropertiesHolder.EXECUTION_LOGS_MARKER, "Total datasets deleted: {} ",
        deletedCounter);
  }

  private void convertLineToDataset(String[] line) {
    try {
      Dataset dataset = extractDatasetColumnsFromArray(line);
      Workflow workflow = extractWorkflowColumnsFromArray(line);
      Dataset storedDataset = null;
      String datasetIdString = null;
      if (dataset != null) {
        storedDataset = datasetDao.getDatasetByDatasetId(dataset.getDatasetId());
        datasetIdString = dataset.getDatasetId();
      }

      if (dataset != null && storedDataset == null && workflow != null) {
        workflow.setDatasetId(dataset.getDatasetId());
        datasetDao.create(dataset);
        workflowDao.create(workflow);
        storedCounter++;
        LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Line: {}, Dataset with datasetId: {}, created", readCounter, datasetIdString);
        LOGGER.info(PropertiesHolder.SUCCESSFULL_DATASET_IDS, datasetIdString);
      } else if (storedDataset != null) {
        LOGGER.warn(PropertiesHolder.FAILED_CSV_LINES_DATASET_ALREADY_EXISTS_MARKER,
            "Line: {}, Dataset with datasetId: {}, already exists", readCounter,
            datasetIdString);
      } else {
        LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER,
            "Line: {}, Failed to read dataset from csv line",
            readCounter);
        failedCounter++;
      }
    } catch (ParseException e) {
      LOGGER.warn(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Line: {}, Failed to read dataset from csv line.", readCounter);
      LOGGER.warn(PropertiesHolder.FAILED_CSV_LINES_MARKER, Arrays.toString(line));
      failedCounter++;
    }
  }

  private Dataset extractDatasetColumnsFromArray(String[] line) throws ParseException {
    Dataset dataset = new Dataset();
    dataset.setEcloudDatasetId(String.format("NOT_CREATED_YET-%s", UUID.randomUUID().toString()));
    //Only get the first numeric part of the Columns.NAME field
    Pattern pattern = Pattern.compile("^(\\d+[a-zA-Z]*\\d?)_.*|^(\\d+[a-zA-Z]*\\d?)$");
    Matcher matcher = pattern.matcher(line[Columns.NAME.getIndex()].trim());
    if (matcher.find()) {
      String datasetId = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
      dataset.setDatasetId(datasetId);
      if (datasetId.charAt(0) == '0') {
        LOGGER.info(PropertiesHolder.LEADING_ZEROS_DATASET_IDS_MARKER, datasetId);
      }
    } else {
      return null;
    }
    dataset.setDatasetName(line[Columns.NAME.getIndex()].trim());
    dataset.setOrganizationId(propertiesHolder.organizationId);
    dataset.setOrganizationName(propertiesHolder.organizationName);
    dataset.setCreatedByUserId(propertiesHolder.userId);
    SimpleDateFormat dateFormat = new SimpleDateFormat(PropertiesHolder.SUGARCRM_DATE_FORMAT);
    dataset.setCreatedDate(dateFormat.parse(line[Columns.DATE_CREATED.getIndex()].trim()));
    dataset.setCountry(
        Country.getCountryFromIsoCode(line[Columns.DATASET_COUNTRY_CODE.getIndex()].trim()));
    dataset.setDescription(line[Columns.DESCRIPTION.getIndex()]);
    dataset.setNotes(line[Columns.NOTES.getIndex()]);
    return dataset;
  }

  private Workflow extractWorkflowColumnsFromArray(String[] line) {
    List<AbstractMetisPluginMetadata> abstractMetisPluginMetadata = new ArrayList<>(1);
    if (line[Columns.HARVEST_TYPE.getIndex()].trim().equals("oai_pmh")) {
      OaipmhHarvestPluginMetadata oaipmhHarvestPluginMetadata = new OaipmhHarvestPluginMetadata();
      oaipmhHarvestPluginMetadata.setUrl(
          isValid(line[Columns.HARVEST_URL.getIndex()]) ? line[Columns.HARVEST_URL.getIndex()]
              .trim() : null);
      oaipmhHarvestPluginMetadata
          .setMetadataFormat(line[Columns.METADATA_FORMAT.getIndex()].trim());
      oaipmhHarvestPluginMetadata
          .setSetSpec(line[Columns.SETSPEC.getIndex()].trim().equals("-") ? null
              : line[Columns.SETSPEC.getIndex()].trim());
      oaipmhHarvestPluginMetadata.setMocked(false);
      oaipmhHarvestPluginMetadata.setEnabled(true);
      abstractMetisPluginMetadata.add(oaipmhHarvestPluginMetadata);
    } else { //Http type of any other, and create an http plugin with or without a valid url
      HTTPHarvestPluginMetadata httpHarvestPluginMetadata = new HTTPHarvestPluginMetadata();
      httpHarvestPluginMetadata.setUrl(
          isValid(line[Columns.HTTP_URL.getIndex()]) ? line[Columns.HTTP_URL.getIndex()].trim()
              : null);
      httpHarvestPluginMetadata.setMocked(false);
      httpHarvestPluginMetadata.setEnabled(true);
      abstractMetisPluginMetadata.add(httpHarvestPluginMetadata);
    }

    Workflow workflow = new Workflow();
    workflow.setMetisPluginsMetadata(abstractMetisPluginMetadata);
    return abstractMetisPluginMetadata.isEmpty() ? null : workflow;
  }

  private static boolean isValid(String url) {
    try {
      new URL(url).toURI();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
