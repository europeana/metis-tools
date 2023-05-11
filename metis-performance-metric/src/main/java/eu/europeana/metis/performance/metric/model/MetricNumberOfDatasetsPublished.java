package eu.europeana.metis.performance.metric.model;

import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import eu.europeana.metis.performance.metric.utilities.CSVUtilities;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class is responsible in calculating the number of records and datasets published within a date interval
 * and how long it took to process each dataset
 */

public class MetricNumberOfDatasetsPublished extends Metric {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricNumberOfDatasetsPublished.class);
    private final SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final MongoMetisCoreDao mongoMetisCoreDao;
    private List<String> metricContent;

    public MetricNumberOfDatasetsPublished(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    @Override
    public void processMetric(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime) {
        final Date startDate = Date.from(startLocalDateTime.atZone(ZoneId.systemDefault()).toInstant());
        final Date endDate = Date.from(endLocalDateTime.atZone(ZoneId.systemDefault()).toInstant());
        final List<WorkflowExecutionDao.ExecutionDatasetPair> workflowExecutionsOverview =
                mongoMetisCoreDao.getAllWorkflowsExecutionsOverviewThatFinished(startDate, endDate).getResults();
        List<WorkflowExecution> cleanedWorkflowExecutionList = cleanUpWorkflowExecutionList(workflowExecutionsOverview);
        cleanedWorkflowExecutionList.sort(Comparator.comparing(workflow -> Integer.parseInt(workflow.getDatasetId())));

        metricContent = cleanedWorkflowExecutionList.stream()
                .map(this::turnDatasetIntoCSVRowForMetric2)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());

    }

    @Override
    public void toCsv(String filePath) {
        final File resultFile = new File(filePath);
        final String firstRow = "DatasetId, Date of Index to Publish, Time since last successful harvest in hours, Number of Records Published";


        if (metricContent.isEmpty()) {
            LOGGER.error("There is no content to print");
        } else {
            CSVUtilities.printIntoFile(resultFile, firstRow, metricContent);
        }
    }

    private List<WorkflowExecution> cleanUpWorkflowExecutionList(List<WorkflowExecutionDao.ExecutionDatasetPair> listToClean) {
        List<WorkflowExecution> result = new ArrayList<>();
        final List<Dataset> uniqueDatasets = listToClean.stream()
                .map(WorkflowExecutionDao.ExecutionDatasetPair::getDataset)
                .collect(Collectors.collectingAndThen(
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing((Dataset::getDatasetId)))), ArrayList::new));

        for (Dataset dateset : uniqueDatasets) {
            result.add(listToClean.stream()
                    .filter(pair -> pair.getDataset().getDatasetId().equals(dateset.getDatasetId()))
                    .map(WorkflowExecutionDao.ExecutionDatasetPair::getExecution)
                    .collect(Collectors.toList()).get(0));
        }

        return result;

    }

    private String turnDatasetIntoCSVRowForMetric2(WorkflowExecution execution) {
        final String datasetId = execution.getDatasetId();

        LOGGER.info("Processing dataset with id {} for metric 2", datasetId);
        final StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(datasetId);
        stringBuilderCSVRow.append(", ");
        final Optional<AbstractMetisPlugin> optionalPublishPlugin = execution.getMetisPluginWithType(PluginType.PUBLISH);
        if (optionalPublishPlugin.isPresent()) {
            final AbstractMetisPlugin publishPlugin = optionalPublishPlugin.get();
            stringBuilderCSVRow.append(simpleDateTimeFormat.format(publishPlugin.getFinishedDate()));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculateTimeDifference(execution, (AbstractExecutablePlugin<?>) publishPlugin));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculatePublishRecords((AbstractExecutablePlugin<?>) publishPlugin));
        } else {
            LOGGER.error("Something went wrong when extracting data");
            return "";
        }

        LOGGER.info("Finished processing dataset with id {} for metric 2", datasetId);

        return stringBuilderCSVRow.toString();
    }

    private int calculatePublishRecords(AbstractExecutablePlugin<?> publishPlugin) {
        return publishPlugin.getExecutionProgress().getProcessedRecords() - publishPlugin.getExecutionProgress().getErrors();
    }

    private long calculateTimeDifference(WorkflowExecution workflowExecution, AbstractExecutablePlugin<?> publishPlugin) {
        final PluginWithExecutionId<? extends ExecutablePlugin> harvestPlugin =
                mongoMetisCoreDao.getHarvesting(new PluginWithExecutionId<>(workflowExecution, publishPlugin));
        return TimeUnit.MILLISECONDS.toHours(publishPlugin.getFinishedDate().getTime() - harvestPlugin.getPlugin().getStartedDate().getTime());
    }
}
