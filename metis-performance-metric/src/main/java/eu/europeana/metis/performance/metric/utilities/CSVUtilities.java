package eu.europeana.metis.performance.metric.utilities;


import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CSVUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);

    private final MongoMetisCoreDao mongoMetisCoreDao;
    public CSVUtilities(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    public void writeIntoCsvFile(String filePath, Date startDate, Date endDate) throws FileNotFoundException {
        File resultFile = new File(filePath);
        List<Dataset> datasetsToProcess = mongoMetisCoreDao.getAllDatasetsWithinDateInterval(startDate, endDate);
        String firstRow = "DatasetId, Time of last Successful harvest, Date of Indexing to Publish, Number of Records Published";

        try (PrintWriter printWriter = new PrintWriter(resultFile)) {
            List<String> contentToPrint = datasetsToProcess.stream().map(this::turnDatasetIntoCSVRow).collect(Collectors.toList());
            printWriter.println(firstRow);
            contentToPrint.forEach(content -> {
                if(StringUtils.isNotEmpty(content)){
                    printWriter.println(content);
                }
            });
        }

    }

    private String turnDatasetIntoCSVRow(Dataset dataset){
        LOGGER.info("Processing dataset with id " + dataset.getDatasetId());
        WorkflowExecution lastSuccessfulWorkflow = mongoMetisCoreDao.getLatestSuccessfulWorkflowWithDatasetId(dataset.getDatasetId());

        if(lastSuccessfulWorkflow == null){
            return "";
        }

        Optional<AbstractMetisPlugin> optionalPublishPlugin = lastSuccessfulWorkflow.getMetisPluginWithType(PluginType.PUBLISH);
        SimpleDateFormat simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(dataset.getDatasetId());
        stringBuilderCSVRow.append(", ");
        stringBuilderCSVRow.append(simpleDateFormatter.format(lastSuccessfulWorkflow.getStartedDate()));
        stringBuilderCSVRow.append(", ");

        if(optionalPublishPlugin.isPresent()){
            AbstractExecutablePlugin publishPlugin = (AbstractExecutablePlugin<?>) optionalPublishPlugin.get();
            stringBuilderCSVRow.append(simpleDateFormatter.format(publishPlugin.getStartedDate()));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(publishPlugin.getExecutionProgress().getProcessedRecords());
        } else {
            stringBuilderCSVRow.append("null, null");
        }

        LOGGER.info("Finished processing dataset with id " + dataset.getDatasetId());

        return stringBuilderCSVRow.toString();
    }

}
