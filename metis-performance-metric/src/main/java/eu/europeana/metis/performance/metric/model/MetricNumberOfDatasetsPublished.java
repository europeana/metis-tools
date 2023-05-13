package eu.europeana.metis.performance.metric.model;

import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import eu.europeana.metis.performance.metric.utilities.CSVUtilities;
import java.io.File;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                mongoMetisCoreDao.getAllSuccessfulPublishWorkflows(startDate, endDate).getResults();
        final List<List<Pair<ExecutablePlugin, WorkflowExecution>>> allEvolutions = workflowExecutionsOverview
            .stream().map(this::getEvolutionForPublishAction).filter(Objects::nonNull).collect(Collectors.toList());
        List<List<Pair<ExecutablePlugin, WorkflowExecution>>> cleanedWorkflowExecutionList = cleanUpWorkflowExecutionList(allEvolutions);
        cleanedWorkflowExecutionList.sort(Comparator.comparing(evolution -> evolution.get(0).getLeft().getStartedDate()));
        metricContent = cleanedWorkflowExecutionList.stream()
                .map(this::turnDatasetIntoCSVRowForMetric2)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
    }

    @Override
    public void toCsv(String filePath) {
        final File resultFile = new File(filePath);
        final String firstRow = "DatasetId, Date of Index to Publish, Time since last successful harvest in hours,"
            + " Total processing time in seconds, Number of Records Published";

        if (metricContent.isEmpty()) {
            LOGGER.error("There is no content to print");
        } else {
            CSVUtilities.printIntoFile(resultFile, firstRow, metricContent);
        }
    }

    private List<Pair<ExecutablePlugin, WorkflowExecution>> getEvolutionForPublishAction(
            WorkflowExecutionDao.ExecutionDatasetPair publishPair) {
        final String datasetId = publishPair.getDataset().getDatasetId();
        LOGGER.info("Processing evolution for dataset with id {} for metric 2", datasetId);
        final Optional<AbstractMetisPlugin> optionalPublishPlugin = publishPair.getExecution()
            .getMetisPluginWithType(PluginType.PUBLISH);
        if (optionalPublishPlugin.isEmpty()) {
            LOGGER.error("Something went wrong when extracting data: non-publish plugin found.");
            return null;
        }
        final ExecutablePlugin publishPlugin = (ExecutablePlugin) optionalPublishPlugin.get();
        final List<Pair<ExecutablePlugin, WorkflowExecution>> result = mongoMetisCoreDao
            .getEvolution(publishPlugin, publishPair.getExecution());
        LOGGER.info("Finished processing evolution for dataset with id {} for metric 2", datasetId);
        return result;
    }

    private List<List<Pair<ExecutablePlugin, WorkflowExecution>>> cleanUpWorkflowExecutionList(
            List<List<Pair<ExecutablePlugin, WorkflowExecution>>> listToClean) {
        final Map<String, List<Pair<ExecutablePlugin, WorkflowExecution>>> lastEvolutionPerHarvest = new HashMap<>();
        listToClean.forEach(evolution -> {
            final String harvestId = evolution.get(0).getLeft().getId();
            final var oldValue = lastEvolutionPerHarvest.get(harvestId);
            if (oldValue != null) {
                final var publishTimeOld = oldValue.get(oldValue.size() - 1).getLeft().getStartedDate();
                final var publishTimeNew = evolution.get(oldValue.size() - 1).getLeft().getStartedDate();
                if (publishTimeOld.toInstant().isBefore(publishTimeNew.toInstant())) {
                    lastEvolutionPerHarvest.put(harvestId, evolution);
                }
            } else {
                lastEvolutionPerHarvest.put(harvestId, evolution);
            }
        });
        return new ArrayList<>(lastEvolutionPerHarvest.values());
    }

    private String turnDatasetIntoCSVRowForMetric2(List<Pair<ExecutablePlugin, WorkflowExecution>> evolution) {
        final String datasetId = evolution.get(0).getRight().getDatasetId();
        final StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(datasetId);
        stringBuilderCSVRow.append(", ");
        if (!Set.of(PluginType.OAIPMH_HARVEST, PluginType.HTTP_HARVEST)
            .contains(evolution.get(0).getLeft().getPluginType())) {
            LOGGER.error("No harvest associated with this publish: Dataset {}", datasetId);
            return "";
        }
        final ExecutablePlugin publishPlugin = evolution.get(evolution.size() - 1).getLeft();
        stringBuilderCSVRow.append(simpleDateTimeFormat.format(publishPlugin.getFinishedDate()));
        stringBuilderCSVRow.append(", ");
        stringBuilderCSVRow.append(calculateTimeDifference(evolution.get(0).getLeft(), publishPlugin));
        stringBuilderCSVRow.append(", ");
        stringBuilderCSVRow.append(calculateRunningTime(evolution));
        stringBuilderCSVRow.append(", ");
        stringBuilderCSVRow.append(calculatePublishRecords((AbstractExecutablePlugin<?>) publishPlugin));
        return stringBuilderCSVRow.toString();
    }

    private int calculatePublishRecords(AbstractExecutablePlugin<?> publishPlugin) {
        return publishPlugin.getExecutionProgress().getProcessedRecords() - publishPlugin.getExecutionProgress().getErrors();
    }

    private long calculateTimeDifference(ExecutablePlugin harvestPlugin, ExecutablePlugin publishPlugin) {
        return TimeUnit.MILLISECONDS.toHours(publishPlugin.getFinishedDate().getTime() - harvestPlugin.getStartedDate().getTime());
    }

    private long calculateRunningTime(List<Pair<ExecutablePlugin, WorkflowExecution>> evolution) {
        return evolution.stream().map(Pair::getLeft).mapToLong(plugin -> plugin.getStartedDate().toInstant()
            .until(plugin.getFinishedDate().toInstant(), ChronoUnit.SECONDS)).sum();
    }
}
