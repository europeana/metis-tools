package eu.europeana.metis.performance.metric.utilities;

import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PerformanceMetricsUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);

    private final MongoMetisCoreDao mongoMetisCoreDao;

    public PerformanceMetricsUtilities(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    public List<String> getDateForMetric1(Date startDate, Date endDate){
//        List<WorkflowExecution> workflowExecutionsOverview =
//                mongoMetisCoreDao.getAllWorkflowsExecutionsOverview(startDate, endDate).getResults()
//                        .stream()
//                        .map(WorkflowExecutionDao.ExecutionDatasetPair::getExecution);


        return null;
    }

    public List<String> getDataForMetric2(Date startDate, Date endDate){
        List<WorkflowExecutionDao.ExecutionDatasetPair> workflowExecutionsOverview =
                mongoMetisCoreDao.getAllWorkflowsExecutionsOverview(startDate, endDate).getResults();
        List<WorkflowExecution> cleanedWorkflowExecutionList = cleanUpWorkflowExecutionList(workflowExecutionsOverview);
        cleanedWorkflowExecutionList.sort(Comparator.comparing(workflow -> Integer.parseInt(workflow.getDatasetId())));

        return cleanedWorkflowExecutionList.stream()
                .map(this::turnDatasetIntoCSVRowForMetric2)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
    }

    private List<WorkflowExecution> cleanUpWorkflowExecutionList(List<WorkflowExecutionDao.ExecutionDatasetPair> listToClean){
        List<WorkflowExecution> result = new ArrayList<>();
        List<Dataset> uniqueDatasets = listToClean.stream()
                .map(WorkflowExecutionDao.ExecutionDatasetPair::getDataset)
                .collect(Collectors.collectingAndThen(
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing((Dataset::getDatasetId)))), ArrayList::new));

        for (Dataset dateset: uniqueDatasets) {
            result.add(listToClean.stream()
                    .filter(pair -> pair.getDataset().getDatasetId().equals(dateset.getDatasetId()))
                    .map(WorkflowExecutionDao.ExecutionDatasetPair::getExecution)
                    .collect(Collectors.toList()).get(0));
        }

        return result;

    }

    private String turnDatasetIntoCSVRowForMetric2(WorkflowExecution execution){
        final String datasetId = execution.getDatasetId();
        final SimpleDateFormat simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        LOGGER.info("Processing dataset with id {}", datasetId);
        final StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(datasetId);
        stringBuilderCSVRow.append(", ");
        final Optional<AbstractMetisPlugin> optionalPublishPlugin = execution.getMetisPluginWithType(PluginType.PUBLISH);
        if(optionalPublishPlugin.isPresent()){
            final AbstractMetisPlugin publishPlugin = optionalPublishPlugin.get();
            stringBuilderCSVRow.append(simpleDateFormatter.format(publishPlugin.getFinishedDate()));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculateTimeDifference(execution, (AbstractExecutablePlugin<?>)publishPlugin));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculatePublishRecords((AbstractExecutablePlugin<?>) publishPlugin));
        } else {
            LOGGER.error("Something went wrong when extracting data");
            return "";
        }

        LOGGER.info("Finished processing dataset with id {}", datasetId);

        return stringBuilderCSVRow.toString();
    }

    private int calculatePublishRecords(AbstractExecutablePlugin<?> publishPlugin){
        return publishPlugin.getExecutionProgress().getProcessedRecords() - publishPlugin.getExecutionProgress().getErrors();
    }

    private long calculateTimeDifference(WorkflowExecution workflowExecution, AbstractExecutablePlugin<?> publishPlugin){
        PluginWithExecutionId<? extends ExecutablePlugin> harvestPlugin =
                mongoMetisCoreDao.getHarvesting(new PluginWithExecutionId<>(workflowExecution,publishPlugin));
        return TimeUnit.MILLISECONDS.toHours(publishPlugin.getFinishedDate().getTime() - harvestPlugin.getPlugin().getStartedDate().getTime());
    }

}
