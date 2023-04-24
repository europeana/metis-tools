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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CSVUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);

    private final MongoMetisCoreDao mongoMetisCoreDao;
    public CSVUtilities(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    public void writeIntoCsvFile(String filePath, Date startDate, Date endDate) throws FileNotFoundException {
        File resultFile = new File(filePath);
        String firstRow = "DatasetId, Date Publish Indexing, Time of last successful harvest in hours, Number of Records Published";

        try (PrintWriter printWriter = new PrintWriter(resultFile)) {
            List<WorkflowExecutionDao.ExecutionDatasetPair> workflowExecutionsOverview =
                    mongoMetisCoreDao.getAllWorkflowsExecutionsOverview(startDate, endDate).getResults();

            List<String> contentToPrint = workflowExecutionsOverview.stream()
                    .map(this::turnDatasetIntoCSVRow)
                    .filter(StringUtils::isNotEmpty)
                    .collect(Collectors.toList());
            Collections.reverse(contentToPrint);
            printWriter.println(firstRow);
            contentToPrint.forEach(content -> {
                if(StringUtils.isNotEmpty(content)){
                    printWriter.println(content);
                }
            });
        }

    }

    private String turnDatasetIntoCSVRow(WorkflowExecutionDao.ExecutionDatasetPair executionDatasetPair){
        Dataset dataset = executionDatasetPair.getDataset();
        WorkflowExecution execution = executionDatasetPair.getExecution();
        SimpleDateFormat simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        LOGGER.info("Processing dataset with id " + dataset.getDatasetId());
        StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(dataset.getDatasetId());
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

        LOGGER.info("Finished processing dataset with id " + dataset.getDatasetId());

        return stringBuilderCSVRow.toString();
    }

    private long calculateTimeDifference(WorkflowExecution workflowExecution, AbstractExecutablePlugin<?> publishPlugin){
        PluginWithExecutionId<? extends ExecutablePlugin> harvestPlugin =
                mongoMetisCoreDao.getHarvesting(new PluginWithExecutionId<>(workflowExecution,publishPlugin));
        return TimeUnit.MILLISECONDS.toHours(publishPlugin.getFinishedDate().getTime() - harvestPlugin.getPlugin().getStartedDate().getTime());
    }

}
