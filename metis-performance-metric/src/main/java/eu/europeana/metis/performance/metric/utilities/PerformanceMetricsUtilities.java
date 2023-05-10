package eu.europeana.metis.performance.metric.utilities;

import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.WorkflowExecution;

import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Optional;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PerformanceMetricsUtilities {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVUtilities.class);
    private static final SimpleDateFormat SIMPLE_DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private final MongoMetisCoreDao mongoMetisCoreDao;

    public PerformanceMetricsUtilities(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    public List<String> getDateForMetric1(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime){
        final StringBuilder stringBuilder = new StringBuilder();
        LocalDateTime currentDate = startLocalDateTime;
        final Map<LocalDateTime, List<String>> dataByDate = new HashMap<>();

        while(currentDate.isBefore(endLocalDateTime)){
            List<String> values = prepareDataWithDate(currentDate);
            if(CollectionUtils.isNotEmpty(values) && allElementsAreNotZero(values)) {
                dataByDate.put(currentDate, values);
            }
            currentDate = currentDate.plusDays(1L);
        }

        List<String> valuesForEndDate = prepareDataWithDate(endLocalDateTime);
        if(CollectionUtils.isNotEmpty(valuesForEndDate) && allElementsAreNotZero(valuesForEndDate)) {
            dataByDate.put(currentDate, valuesForEndDate);
        }

        return dataByDate.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> {
                    Date date =  Date.from(entry.getKey().atZone(ZoneId.systemDefault()).toInstant());
                    stringBuilder.setLength(0);
                    stringBuilder.append(SIMPLE_DATE_FORMAT.format(date));
                    stringBuilder.append(", ");
                    stringBuilder.append(String.join(", ", entry.getValue()));
                    return stringBuilder.toString();
                })
                .collect(Collectors.toList());
    }

    public List<String> getDataForMetric2(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime){
        final Date startDate = Date.from(startLocalDateTime.atZone(ZoneId.systemDefault()).toInstant());
        final Date endDate = Date.from(endLocalDateTime.atZone(ZoneId.systemDefault()).toInstant());
        final List<WorkflowExecutionDao.ExecutionDatasetPair> workflowExecutionsOverview =
                mongoMetisCoreDao.getAllWorkflowsExecutionsOverviewThatFinished(startDate, endDate).getResults();
        List<WorkflowExecution> cleanedWorkflowExecutionList = cleanUpWorkflowExecutionList(workflowExecutionsOverview);
        cleanedWorkflowExecutionList.sort(Comparator.comparing(workflow -> Integer.parseInt(workflow.getDatasetId())));

        return cleanedWorkflowExecutionList.stream()
                .map(this::turnDatasetIntoCSVRowForMetric2)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());
    }

    private List<String> prepareDataWithDate(LocalDateTime dateToGatherData){
        final Date startDate = Date.from(dateToGatherData.atZone(ZoneId.systemDefault()).toInstant());
        final Date endDate = Date.from(dateToGatherData.plusDays(1L).atZone(ZoneId.systemDefault()).toInstant());
        LOGGER.info("Processing data for metric 1  date: {}", SIMPLE_DATE_FORMAT.format(startDate));
        final List<WorkflowExecution> result = mongoMetisCoreDao.getAllWorkflowsWithinDateInterval(startDate, endDate).getResults()
                .stream()
                .map(WorkflowExecutionDao.ExecutionDatasetPair::getExecution)
                .collect(Collectors.toList());
        if(CollectionUtils.isNotEmpty(result)) {
            final String oaiPmh = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.OAIPMH_HARVEST,result, startDate, endDate));
            final String httpHarvest = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.HTTP_HARVEST,result, startDate, endDate));
            final String validateEdmExternal = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.VALIDATION_EXTERNAL,result, startDate, endDate));
            final String transform = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.TRANSFORMATION,result, startDate, endDate));
            final String validateEdmInternal = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.VALIDATION_INTERNAL,result, startDate, endDate));
            final String normalise = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.NORMALIZATION,result, startDate, endDate));
            final String enrich = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.ENRICHMENT,result, startDate, endDate));
            final String processMedia = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.MEDIA_PROCESS,result, startDate, endDate));
            final String indexToPreview = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.PREVIEW,result, startDate, endDate));
            final String indexToPublish = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.PUBLISH,result, startDate, endDate));
            LOGGER.info("Finished data for metric 1  date: {}", SIMPLE_DATE_FORMAT.format(startDate));
            return List.of(oaiPmh, httpHarvest, validateEdmExternal, transform, validateEdmInternal, normalise, enrich, processMedia, indexToPreview, indexToPublish);
        } else {
            LOGGER.info("Finished data for metric 1  date: {}", SIMPLE_DATE_FORMAT.format(startDate));
            return Collections.emptyList();
        }
    }

    private int calculateNumberOfRecordsForPluginType(PluginType pluginType, List<WorkflowExecution> listToGetDataFrom,
                                                      Date startDate, Date endDate){
        return listToGetDataFrom.stream()
                .map(workflowExecution -> workflowExecution.getMetisPluginWithType(pluginType))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(plugin -> !plugin.getPluginStatus().equals(PluginStatus.CANCELLED) && plugin.getDataStatus().equals(DataStatus.VALID))
                .map(plugin -> getNumberOfRecordInDate((AbstractExecutablePlugin<?>) plugin, startDate, endDate))
                .mapToInt(Integer::intValue)
                .sum();
    }

    private int getNumberOfRecordInDate(AbstractExecutablePlugin<?> plugin, Date startDate, Date endDate) {
        final long pluginStartTime = plugin.getStartedDate().getTime();
        final long pluginEndTime = plugin.getFinishedDate() != null ? plugin.getFinishedDate().getTime() : plugin.getUpdatedDate().getTime();

        if(pluginIsOutsideDateInterval(pluginStartTime, pluginEndTime, startDate, endDate)){
            return 0;
        } else if(pluginStartsAndFinishedWithinInterval(pluginStartTime, pluginEndTime, startDate, endDate)) {
            return plugin.getExecutionProgress().getProcessedRecords();

        } else if(pluginStartedBeforeDateInterval(pluginStartTime, pluginEndTime, startDate, endDate)){
            final long totalTime = pluginEndTime - pluginStartTime;
            final long actualTime = pluginEndTime - startDate.getTime();
            return estimateNumberOfRecords(totalTime, actualTime, plugin.getExecutionProgress().getProcessedRecords());

        } else if(pluginFinishesAfterDateInterval(pluginStartTime, pluginEndTime, startDate, endDate)){
            final long totalTime = pluginEndTime - pluginStartTime;
            final long actualTime = pluginStartTime - endDate.getTime();
            return estimateNumberOfRecords(totalTime, actualTime, plugin.getExecutionProgress().getProcessedRecords());
        }

        return 0;
    }

    private boolean pluginStartsAndFinishedWithinInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate){
        return pluginStartTime >= startDate.getTime() &&
                pluginEndTime < endDate.getTime();
    }

    private boolean pluginStartedBeforeDateInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate){
        return pluginStartTime < startDate.getTime() &&
                pluginEndTime < endDate.getTime();
    }

    private boolean pluginFinishesAfterDateInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate){
        return pluginStartTime >= startDate.getTime() &&
                pluginEndTime >= endDate.getTime();
    }

    private boolean pluginIsOutsideDateInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate){
        return (pluginStartTime < startDate.getTime() && pluginEndTime < startDate.getTime()) ||
                (pluginStartTime > endDate.getTime() && pluginEndTime > endDate.getTime());
    }

    private int estimateNumberOfRecords(long totalTime, long actualTime, int totalRecords){
        return (int) Math.ceil((double) (totalRecords * actualTime) / totalTime);
    }

    private boolean allElementsAreNotZero(List<String> list){
        return !list.stream().allMatch(value -> Integer.parseInt(value) == 0);
    }

    private List<WorkflowExecution> cleanUpWorkflowExecutionList(List<WorkflowExecutionDao.ExecutionDatasetPair> listToClean){
        List<WorkflowExecution> result = new ArrayList<>();
        final List<Dataset> uniqueDatasets = listToClean.stream()
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

        LOGGER.info("Processing dataset with id {} for metric 2", datasetId);
        final StringBuilder stringBuilderCSVRow = new StringBuilder();
        stringBuilderCSVRow.append(datasetId);
        stringBuilderCSVRow.append(", ");
        final Optional<AbstractMetisPlugin> optionalPublishPlugin = execution.getMetisPluginWithType(PluginType.PUBLISH);
        if(optionalPublishPlugin.isPresent()){
            final AbstractMetisPlugin publishPlugin = optionalPublishPlugin.get();
            stringBuilderCSVRow.append(SIMPLE_DATE_TIME_FORMAT.format(publishPlugin.getFinishedDate()));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculateTimeDifference(execution, (AbstractExecutablePlugin<?>)publishPlugin));
            stringBuilderCSVRow.append(", ");
            stringBuilderCSVRow.append(calculatePublishRecords((AbstractExecutablePlugin<?>) publishPlugin));
        } else {
            LOGGER.error("Something went wrong when extracting data");
            return "";
        }

        LOGGER.info("Finished processing dataset with id {} for metric 2", datasetId);

        return stringBuilderCSVRow.toString();
    }

    private int calculatePublishRecords(AbstractExecutablePlugin<?> publishPlugin){
        return publishPlugin.getExecutionProgress().getProcessedRecords() - publishPlugin.getExecutionProgress().getErrors();
    }

    private long calculateTimeDifference(WorkflowExecution workflowExecution, AbstractExecutablePlugin<?> publishPlugin){
        final PluginWithExecutionId<? extends ExecutablePlugin> harvestPlugin =
                mongoMetisCoreDao.getHarvesting(new PluginWithExecutionId<>(workflowExecution,publishPlugin));
        return TimeUnit.MILLISECONDS.toHours(publishPlugin.getFinishedDate().getTime() - harvestPlugin.getPlugin().getStartedDate().getTime());
    }

}
