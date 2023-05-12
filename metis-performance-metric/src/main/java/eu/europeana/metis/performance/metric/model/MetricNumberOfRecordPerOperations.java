package eu.europeana.metis.performance.metric.model;

import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.performance.metric.dao.MongoMetisCoreDao;
import eu.europeana.metis.performance.metric.utilities.CSVUtilities;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class is responsible in calculating the number of records processed per task per day
 */

public class MetricNumberOfRecordPerOperations extends Metric {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricNumberOfRecordPerOperations.class);
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private final MongoMetisCoreDao mongoMetisCoreDao;
    private List<String> metricContent;

    public MetricNumberOfRecordPerOperations(MongoMetisCoreDao mongoMetisCoreDao) {
        this.mongoMetisCoreDao = mongoMetisCoreDao;
    }

    @Override
    public void processMetric(LocalDateTime startLocalDateTime, LocalDateTime endLocalDateTime) {

        final StringBuilder stringBuilder = new StringBuilder();
        LocalDateTime currentDate = startLocalDateTime;
        final Map<LocalDateTime, List<String>> dataByDate = new HashMap<>();

        while (currentDate.isBefore(endLocalDateTime)) {
            List<String> values = prepareDataWithDate(currentDate);
            if (CollectionUtils.isNotEmpty(values) && allElementsAreNotZero(values)) {
                dataByDate.put(currentDate, values);
            }
            currentDate = currentDate.plusDays(1L);
        }

        List<String> valuesForEndDate = prepareDataWithDate(endLocalDateTime);
        if (CollectionUtils.isNotEmpty(valuesForEndDate) && allElementsAreNotZero(valuesForEndDate)) {
            dataByDate.put(currentDate, valuesForEndDate);
        }

        metricContent = dataByDate.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> {
                    Date date = Date.from(entry.getKey().atZone(ZoneId.systemDefault()).toInstant());
                    stringBuilder.setLength(0);
                    stringBuilder.append(simpleDateFormat.format(date));
                    stringBuilder.append(", ");
                    stringBuilder.append(String.join(", ", entry.getValue()));
                    return stringBuilder.toString();
                })
                .collect(Collectors.toList());

    }

    @Override
    public void toCsv(String filePath) {
        final File resultFile = new File(filePath);
        final String firstRow = "Date, Import OAI-PMH, Import HTTP, Validate EDM External, Transformation, Validate EDM Internal, " +
                "Normalization, Enrichment, Media Processing, Index to Preview, Index to Publish";

        if (metricContent.isEmpty()) {
            LOGGER.error("There is no content to print");
        } else {
            CSVUtilities.printIntoFile(resultFile, firstRow, metricContent);
        }

    }

    private List<String> prepareDataWithDate(LocalDateTime dateToGatherData) {
        final Date startDate = Date.from(dateToGatherData.atZone(ZoneId.systemDefault()).toInstant());
        final Date endDate = Date.from(dateToGatherData.plusDays(1L).atZone(ZoneId.systemDefault()).toInstant());
        final String formattedStartDate = simpleDateFormat.format(startDate);
        LOGGER.info("Processing data for metric 1  date: {}", formattedStartDate);
        final List<WorkflowExecution> result = mongoMetisCoreDao.getAllWorkflowsWithinDateInterval(startDate, endDate).getResults()
                .stream()
                .map(WorkflowExecutionDao.ExecutionDatasetPair::getExecution)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(result)) {
            final String oaiPmh = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.OAIPMH_HARVEST, result, startDate, endDate));
            final String httpHarvest = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.HTTP_HARVEST, result, startDate, endDate));
            final String validateEdmExternal = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.VALIDATION_EXTERNAL, result, startDate, endDate));
            final String transform = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.TRANSFORMATION, result, startDate, endDate));
            final String validateEdmInternal = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.VALIDATION_INTERNAL, result, startDate, endDate));
            final String normalise = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.NORMALIZATION, result, startDate, endDate));
            final String enrich = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.ENRICHMENT, result, startDate, endDate));
            final String processMedia = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.MEDIA_PROCESS, result, startDate, endDate));
            final String indexToPreview = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.PREVIEW, result, startDate, endDate));
            final String indexToPublish = String.valueOf(calculateNumberOfRecordsForPluginType(PluginType.PUBLISH, result, startDate, endDate));
            LOGGER.info("Finished data for metric 1  date: {}", formattedStartDate);
            return List.of(oaiPmh, httpHarvest, validateEdmExternal, transform, validateEdmInternal, normalise, enrich, processMedia, indexToPreview, indexToPublish);
        } else {
            LOGGER.info("Finished data for metric 1  date: {}", formattedStartDate);
            return Collections.emptyList();
        }
    }

    private int calculateNumberOfRecordsForPluginType(PluginType pluginType, List<WorkflowExecution> listToGetDataFrom,
                                                      Date startDate, Date endDate) {
        return listToGetDataFrom.stream()
                .map(workflowExecution -> workflowExecution.getMetisPluginWithType(pluginType))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(plugin -> getNumberOfRecordInDate((AbstractExecutablePlugin<?>) plugin, startDate, endDate))
                .mapToInt(Integer::intValue)
                .sum();
    }

    private int getNumberOfRecordInDate(AbstractExecutablePlugin<?> plugin, Date startDate, Date endDate) {
        if(plugin.getStartedDate() == null){
            return 0;
        } else if (plugin.getStartedDate() != null && plugin.getFinishedDate() == null
                && plugin.getStartedDate().getTime() >=  startDate.getTime()){
            return plugin.getExecutionProgress().getProcessedRecords() + plugin.getExecutionProgress().getErrors();
        }

        final long pluginStartTime = plugin.getStartedDate().getTime();
        final long pluginEndTime = plugin.getFinishedDate() != null ? plugin.getFinishedDate().getTime() : plugin.getUpdatedDate().getTime();

        if (pluginIsOutsideDateInterval(pluginStartTime, pluginEndTime, startDate, endDate)) {
            return 0;
        } else if (pluginStartsAndFinishedWithinInterval(pluginStartTime, pluginEndTime, startDate, endDate)) {
            return plugin.getExecutionProgress().getProcessedRecords() + plugin.getExecutionProgress().getErrors();

        } else if (pluginStartedBeforeDateInterval(pluginStartTime, pluginEndTime, startDate, endDate)) {
            final long totalTime = pluginEndTime - pluginStartTime;
            final long actualTime = pluginEndTime - startDate.getTime();
            final int totalRecordNumber = plugin.getExecutionProgress().getProcessedRecords() + plugin.getExecutionProgress().getErrors();
            return estimateNumberOfRecords(totalTime, actualTime, totalRecordNumber);

        } else if (pluginFinishesAfterDateInterval(pluginStartTime, pluginEndTime, startDate, endDate)) {
            final long totalTime = pluginEndTime - pluginStartTime;
            final long actualTime = pluginStartTime - endDate.getTime();
            final int totalRecordNumber = plugin.getExecutionProgress().getProcessedRecords() + plugin.getExecutionProgress().getErrors();
            return estimateNumberOfRecords(totalTime, actualTime, totalRecordNumber);
        }

        return 0;
    }

    private boolean pluginStartsAndFinishedWithinInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate) {
        return pluginStartTime >= startDate.getTime() &&
                pluginEndTime < endDate.getTime();
    }

    private boolean pluginStartedBeforeDateInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate) {
        return pluginStartTime < startDate.getTime() &&
                pluginEndTime < endDate.getTime();
    }

    private boolean pluginFinishesAfterDateInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate) {
        return pluginStartTime >= startDate.getTime() &&
                pluginEndTime >= endDate.getTime();
    }

    private boolean pluginIsOutsideDateInterval(long pluginStartTime, long pluginEndTime, Date startDate, Date endDate) {
        return (pluginStartTime < startDate.getTime() && pluginEndTime < startDate.getTime()) ||
                (pluginStartTime > endDate.getTime() && pluginEndTime > endDate.getTime());
    }

    private int estimateNumberOfRecords(long totalTime, long actualTime, int totalRecords) {
        return (int) Math.ceil((double) (totalRecords * actualTime) / totalTime);
    }

    private boolean allElementsAreNotZero(List<String> list) {
        return !list.stream().allMatch(value -> Integer.parseInt(value) == 0);
    }
}
