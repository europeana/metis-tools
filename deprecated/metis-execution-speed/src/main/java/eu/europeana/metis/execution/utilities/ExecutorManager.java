package eu.europeana.metis.execution.utilities;

import dev.morphia.aggregation.Aggregation;
import dev.morphia.aggregation.stages.Sort;
import dev.morphia.aggregation.stages.Unwind;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filter;
import dev.morphia.query.filters.Filters;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-03-21
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String METIS_PLUGINS = "metisPlugins";
  private static final String PLUGIN_TYPE = "pluginType";
  private static final String PLUGIN_STATUS = "pluginStatus";
  private static final String FINISHED_DATE = "finishedDate";
  private static final String UPDATED_DATE = "updatedDate";
  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final DateFormat dateFormat;
  private static List<PluginType> pluginTypes = null;

  static {
    dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public ExecutorManager(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
    pluginTypes = new ArrayList<>(Arrays.asList(PluginType.values()));
    pluginTypes.remove(PluginType.REINDEX_TO_PREVIEW);
    pluginTypes.remove(PluginType.REINDEX_TO_PUBLISH);
  }

  public void startCalculationForAllPluginTypes(int startNumberOfDaysAgo, int endNumberOfDaysAgo) {
    LocalDateTime currentLocalDateTime = LocalDateTime
        .ofInstant(new Date().toInstant(), ZoneOffset.UTC);
    final LocalDateTime startLocalDateTime = currentLocalDateTime.minusDays(startNumberOfDaysAgo)
        .toLocalDate().atTime(LocalTime.MIN); //Beginning of that day
    final LocalDateTime endLocalDateTime = currentLocalDateTime.minusDays(endNumberOfDaysAgo)
        .toLocalDate().atTime(LocalTime.MAX); //End of that day
    Date fromDate = Date.from(startLocalDateTime.atZone(ZoneOffset.UTC).toInstant());
    Date toDate = Date.from(endLocalDateTime.atZone(ZoneOffset.UTC).toInstant());

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
          "Requesting calculation for all pluginTypes with starting {} days ago which is the date of(UTC): {}, to ending {} days ago which is the date of(UTC): {}",
          startNumberOfDaysAgo, dateFormat.format(fromDate), endNumberOfDaysAgo,
          dateFormat.format(toDate));
    }
    LOGGER.info(PropertiesHolder.STATISTICS_LOGS_MARKER,
        "Calculation for all pluginTypes with starting {} days ago which is the date of(UTC): {}, to ending {} days ago which is the date of(UTC): {}",
        startNumberOfDaysAgo, dateFormat.format(fromDate), endNumberOfDaysAgo,
        dateFormat.format(toDate));

    final AverageMaintainer averageMaintainerFinished = calculationForFinalPluginStatus(
        PluginStatus.FINISHED, fromDate, toDate);
    final AverageMaintainer averageMaintainerCancelled = calculationForFinalPluginStatus(
        PluginStatus.CANCELLED, fromDate, toDate);
    final AverageMaintainer averageMaintainerFailed = calculationForFinalPluginStatus(
        PluginStatus.FAILED, fromDate, toDate);

    final AverageMaintainer allPluginsStatusesAverageMaintainer = new AverageMaintainer();
    allPluginsStatusesAverageMaintainer.addAverageMaintainer(averageMaintainerFinished);
    allPluginsStatusesAverageMaintainer.addAverageMaintainer(averageMaintainerCancelled);
    allPluginsStatusesAverageMaintainer.addAverageMaintainer(averageMaintainerFailed);

    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "All {} pluginTypes and all pluginStatuses together - {}", pluginTypes.size(),
        allPluginsStatusesAverageMaintainer);
    LOGGER.info(PropertiesHolder.STATISTICS_LOGS_MARKER,
        "\nAll {} pluginTypes and all pluginStatuses together - {}", pluginTypes.size(),
        allPluginsStatusesAverageMaintainer);
  }

  /**
   * Calculate the average speed for all plugins that have a provided {@link PluginStatus} and are
   * inside a provided range of dates.
   *
   * @param pluginStatus the plugin status to base the search upon
   * @param fromDate the starting date of the range
   * @param toDate the ending date of the range
   * @return the total average of all plugins
   */
  private AverageMaintainer calculationForFinalPluginStatus(PluginStatus pluginStatus,
      Date fromDate, Date toDate) {
    LOGGER.info(PropertiesHolder.STATISTICS_LOGS_MARKER, "\n{} EXECUTIONS", pluginStatus);

    final AverageMaintainer allPluginsAverageMaintainer = new AverageMaintainer();
    for (PluginType pluginType : pluginTypes) {
      final AverageMaintainer averageMaintainer = calculationForPluginTypeAndFinalStatus(pluginType,
          pluginStatus, fromDate, toDate);
      LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "PluginType: {}, PluginStatus: {} - {}",
          pluginType, pluginStatus, averageMaintainer);
      LOGGER
          .info(PropertiesHolder.STATISTICS_LOGS_MARKER, "{} - {}", pluginType, averageMaintainer);
      allPluginsAverageMaintainer.addAverageMaintainer(averageMaintainer);
    }

    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER,
        "All {} pluginTypes and pluginStatus {} together - {}", pluginTypes.size(), pluginStatus,
        allPluginsAverageMaintainer);
    LOGGER.info(PropertiesHolder.STATISTICS_LOGS_MARKER,
        "All {} pluginTypes and pluginStatus {} together - {}", pluginTypes.size(), pluginStatus,
        allPluginsAverageMaintainer);
    return allPluginsAverageMaintainer;
  }

  private AverageMaintainer calculationForPluginTypeAndFinalStatus(PluginType pluginType,
      PluginStatus pluginStatus, Date fromDate, Date toDate) {
    LOGGER.info(PropertiesHolder.EXECUTION_LOGS_MARKER, "Calculating speed for {} pluginType",
        pluginType);
    final Iterator<WorkflowExecution> iteratorPastSuccessfulPlugins = getIteratorForPluginTypeAndStatus(
        pluginType, pluginStatus, fromDate, toDate);
    return calculateAverageSpeedOfPlugins(iteratorPastSuccessfulPlugins, pluginStatus);
  }

  private Iterator<WorkflowExecution> getIteratorForPluginTypeAndStatus(PluginType pluginType,
      PluginStatus pluginStatus, Date fromDate, Date toDate) {
    Query<WorkflowExecution> query = morphiaDatastoreProvider.getDatastore()
        .find(WorkflowExecution.class);
    query.disableValidation();

    final Aggregation<WorkflowExecution> aggregation = morphiaDatastoreProvider.getDatastore()
        .aggregate(WorkflowExecution.class);

    String dateFieldToCheck = FINISHED_DATE;
    if (pluginStatus == PluginStatus.FAILED || pluginStatus == PluginStatus.CANCELLED) {
      dateFieldToCheck = UPDATED_DATE;
    }

    final Filter pluginTypeFilter = Filters.eq(METIS_PLUGINS + "." + PLUGIN_TYPE, pluginType);
    final Filter pluginStatusFilter = Filters.eq(METIS_PLUGINS + "." + PLUGIN_STATUS, pluginStatus);
    final Filter fromDateFilter = Filters.gte(METIS_PLUGINS + "." + dateFieldToCheck, fromDate);
    final Filter toDateFilter = Filters.lte(METIS_PLUGINS + "." + dateFieldToCheck, toDate);
    final Filter combinedFilters = Filters
        .and(pluginTypeFilter, pluginStatusFilter, fromDateFilter, toDateFilter);

    return aggregation.unwind(Unwind.on(METIS_PLUGINS)).match(combinedFilters)
        .sort(Sort.on().ascending(METIS_PLUGINS + "." + dateFieldToCheck))
        .execute(WorkflowExecution.class);
  }

  /**
   * Given an iterator of {@link WorkflowExecution}s that contain a single {@link
   * AbstractMetisPlugin}, get all statistics from all items and calculate the average speed. During
   * the calculation of {@link PluginStatus#FINISHED} the {@link AbstractMetisPlugin#getFinishedDate()}
   * is used but during the calculation of {@link PluginStatus#CANCELLED} or {@link
   * PluginStatus#FAILED} the {@link AbstractExecutablePlugin#getUpdatedDate()} is used instead.
   *
   * @param iteratorPastSuccessfulPlugins iterator of workflow executions with single plugin
   * @param pluginStatus the final status of the plugins provided
   * @return the result containing the average speed
   */
  private AverageMaintainer calculateAverageSpeedOfPlugins(
      Iterator<WorkflowExecution> iteratorPastSuccessfulPlugins, PluginStatus pluginStatus) {
    final AverageMaintainer averageMaintainer = new AverageMaintainer();
    if (iteratorPastSuccessfulPlugins != null) {
      while (iteratorPastSuccessfulPlugins.hasNext()) {
        final WorkflowExecution workflowExecution = iteratorPastSuccessfulPlugins.next();
        final AbstractMetisPlugin abstractMetisPlugin = workflowExecution.getMetisPlugins().get(0);
        if (abstractMetisPlugin instanceof AbstractExecutablePlugin) {
          final float sampleAverageInSecs = addSampleToCalculation(averageMaintainer,
              (AbstractExecutablePlugin) abstractMetisPlugin, pluginStatus);
          if (LOGGER.isDebugEnabled()) {
            Date endDate = abstractMetisPlugin.getFinishedDate();
            if (pluginStatus == PluginStatus.FAILED || pluginStatus == PluginStatus.CANCELLED) {
              endDate = ((AbstractExecutablePlugin) abstractMetisPlugin).getUpdatedDate();
            }
            LOGGER.debug(PropertiesHolder.EXECUTION_LOGS_MARKER,
                "Started Date: {}, Finished Date: {}, totalRecords: {}, with average speed {} r/s",
                dateFormat.format(abstractMetisPlugin.getStartedDate()), dateFormat.format(endDate),
                ((AbstractExecutablePlugin) abstractMetisPlugin).getExecutionProgress()
                    .getProcessedRecords(), sampleAverageInSecs);
          }
        }
      }
    }
    return averageMaintainer;
  }

  private float addSampleToCalculation(AverageMaintainer averageMaintainer,
      AbstractExecutablePlugin abstractExecutablePlugin, PluginStatus pluginStatus) {
    final Date startedDate = abstractExecutablePlugin.getStartedDate();

    Date endDate = abstractExecutablePlugin.getFinishedDate();
    if (pluginStatus == PluginStatus.FAILED || pluginStatus == PluginStatus.CANCELLED) {
      endDate = abstractExecutablePlugin.getUpdatedDate();
    }
    if (startedDate != null && endDate != null) {
      float executionDurationInSecs = (endDate.getTime() - startedDate.getTime()) / 1000f;
      final int processedRecords = abstractExecutablePlugin.getExecutionProgress()
          .getProcessedRecords();
      if (executionDurationInSecs > 0 && processedRecords > 0) {
        averageMaintainer.addSample(processedRecords, executionDurationInSecs);
      }
      return processedRecords / executionDurationInSecs;
    }
    return 0.0f;
  }
}
