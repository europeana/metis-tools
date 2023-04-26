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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class dedicated to writing content into a csv file
 */
public class CSVUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);

    private final MongoMetisCoreDao mongoMetisCoreDao;
    public CSVUtilities(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    public void writeIntoCsvFile(String filePath, Date startDate, Date endDate) throws FileNotFoundException {
        final File resultFile = new File(filePath);
        final String firstRow = "DatasetId, Date Publish Indexing, Time of last successful harvest in hours, Number of Records Published";

        try (PrintWriter printWriter = new PrintWriter(resultFile)) {
            List<WorkflowExecutionDao.ExecutionDatasetPair> workflowExecutionsOverview =
                    mongoMetisCoreDao.getAllWorkflowsExecutionsOverview(startDate, endDate).getResults();
            List<WorkflowExecution> cleanedWorkflowExecutionList = cleanUpWorkflowExecutionList(workflowExecutionsOverview);
            cleanedWorkflowExecutionList.sort(Comparator.comparing(workflow -> Integer.parseInt(workflow.getDatasetId())));
            List<String> contentToPrint = cleanedWorkflowExecutionList.stream()
                    .map(this::turnDatasetIntoCSVRow)
                    .filter(StringUtils::isNotEmpty)
                    .collect(Collectors.toList());
            printWriter.println(firstRow);
            contentToPrint.forEach(content -> {
                if(StringUtils.isNotEmpty(content)){
                    printWriter.println(content);
                }
            });
        }

    }

    private String turnDatasetIntoCSVRow(WorkflowExecution execution){
        String datasetId = execution.getDatasetId();
        SimpleDateFormat simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        LOGGER.info("Processing dataset with id " + datasetId);
        StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(datasetId);
        stringBuilderCSVRow.append(", ");
        Optional<AbstractMetisPlugin> optionalPublishPlugin = execution.getMetisPluginWithType(PluginType.PUBLISH);
        if(optionalPublishPlugin.isPresent()){
            AbstractMetisPlugin publishPlugin = optionalPublishPlugin.get();
            stringBuilderCSVRow.append(simpleDateFormatter.format(publishPlugin.getFinishedDate()));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculateTimeDifference(execution, (AbstractExecutablePlugin<?>)publishPlugin));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(((AbstractExecutablePlugin<?>) publishPlugin).getExecutionProgress().getProcessedRecords());
        } else {
            LOGGER.error("Something went wrong when extracting data");
            return "";
        }

        LOGGER.info("Finished processing dataset with id " + datasetId);

        return stringBuilderCSVRow.toString();
    }

    private long calculateTimeDifference(WorkflowExecution workflowExecution, AbstractExecutablePlugin<?> publishPlugin){
        PluginWithExecutionId<? extends ExecutablePlugin> harvestPlugin =
                mongoMetisCoreDao.getHarvesting(new PluginWithExecutionId<>(workflowExecution,publishPlugin));
        return TimeUnit.MILLISECONDS.toHours(publishPlugin.getFinishedDate().getTime() - harvestPlugin.getPlugin().getStartedDate().getTime());
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

}
