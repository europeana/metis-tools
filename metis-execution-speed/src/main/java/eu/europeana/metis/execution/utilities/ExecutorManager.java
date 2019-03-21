package eu.europeana.metis.execution.utilities;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import org.mongodb.morphia.aggregation.AggregationPipeline;
import org.mongodb.morphia.query.Criteria;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.Sort;
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
  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final DateFormat dateFormat;

  static {
    dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public ExecutorManager(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
  }

  public AverageMaintainer startCalculation(PluginType pluginType, int startNumberOfDaysAgo,
      int endNumberOfDaysAgo) {
    LocalDateTime currentLocalDateTime = LocalDateTime
        .ofInstant(new Date().toInstant(), ZoneOffset.UTC);
    final LocalDateTime startLocalDateTime = currentLocalDateTime.minusDays(startNumberOfDaysAgo);
    final LocalDateTime endLocalDateTime = currentLocalDateTime.minusDays(endNumberOfDaysAgo);
    Date fromDate = Date.from(startLocalDateTime.atZone(ZoneOffset.UTC).toInstant());
    Date toDate = Date.from(endLocalDateTime.atZone(ZoneOffset.UTC).toInstant());
    LOGGER.info(
        "Requesting calculation for pluginType: {} and starting {} days ago which is the date of(UTC): {}, to ending {} days ago which is the date of(UTC): {}",
        pluginType, startNumberOfDaysAgo, dateFormat.format(fromDate), endNumberOfDaysAgo,
        dateFormat.format(toDate));
    final Iterator<WorkflowExecution> iteratorPastSuccessfulPlugins = getIteratorPastSuccessfulPlugins(
        pluginType, fromDate, toDate);
    return calculateAveragesOfPlugins(iteratorPastSuccessfulPlugins);
  }

  private Iterator<WorkflowExecution> getIteratorPastSuccessfulPlugins(PluginType pluginType,
      Date fromDate, Date toDate) {
    Query<WorkflowExecution> query = morphiaDatastoreProvider.getDatastore()
        .createQuery(WorkflowExecution.class);

    AggregationPipeline aggregation = morphiaDatastoreProvider.getDatastore()
        .createAggregation(WorkflowExecution.class);

    Criteria[] criteria = {
        query.criteria(METIS_PLUGINS + "." + PLUGIN_TYPE).equal(pluginType),
        query.criteria(METIS_PLUGINS + "." + PLUGIN_STATUS).equal(PluginStatus.FINISHED),
        query.criteria(METIS_PLUGINS + "." + FINISHED_DATE).greaterThanOrEq(fromDate),
        query.criteria(METIS_PLUGINS + "." + FINISHED_DATE).lessThanOrEq(toDate)
    };
    query.and(criteria);

    return aggregation.unwind(METIS_PLUGINS).match(query)
        .sort(Sort.ascending(METIS_PLUGINS + "." + FINISHED_DATE))
        .aggregate(WorkflowExecution.class);
  }

  private AverageMaintainer calculateAveragesOfPlugins(
      Iterator<WorkflowExecution> iteratorPastSuccessfulPlugins) {
    final AverageMaintainer averageMaintainer = new AverageMaintainer();
    if (iteratorPastSuccessfulPlugins != null) {
      while (iteratorPastSuccessfulPlugins.hasNext()) {
        final WorkflowExecution workflowExecution = iteratorPastSuccessfulPlugins.next();
        final AbstractMetisPlugin abstractMetisPlugin = workflowExecution.getMetisPlugins().get(0);
        final float sampleAverageInSecs = addSampleToCalculation(averageMaintainer,
            abstractMetisPlugin);
        LOGGER.debug(
            "Started Date: {}, Finished Date: {}, totalRecords: {}, with average speed {} r/s",
            dateFormat.format(abstractMetisPlugin.getStartedDate()),
            dateFormat.format(abstractMetisPlugin.getFinishedDate()),
            abstractMetisPlugin.getExecutionProgress().getProcessedRecords(),
            sampleAverageInSecs);
      }
    }
    return averageMaintainer;
  }

  private float addSampleToCalculation(AverageMaintainer averageMaintainer,
      AbstractMetisPlugin abstractMetisPlugin) {
    final Date startedDate = abstractMetisPlugin.getStartedDate();
    final Date finishedDate = abstractMetisPlugin.getFinishedDate();
    float executionDurationInSecs = (finishedDate.getTime() - startedDate.getTime()) / 1000f;
    final int processedRecords = abstractMetisPlugin.getExecutionProgress().getProcessedRecords();
    averageMaintainer.addSample(processedRecords, executionDurationInSecs);
    return processedRecords / executionDurationInSecs;
  }
}
