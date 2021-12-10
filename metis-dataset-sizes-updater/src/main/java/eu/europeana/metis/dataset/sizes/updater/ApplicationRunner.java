package eu.europeana.metis.dataset.sizes.updater;

import dev.morphia.query.Query;
import dev.morphia.query.experimental.filters.Filters;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.core.dao.PluginWithExecutionId;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
import eu.europeana.metis.mongo.dao.RecordDao;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ApplicationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationRunner.class);
  private static final String ABOUT_FIELD = "about";
  @Value("${dry.run:true}")
  private boolean dryRun;

  private final RecordDao recordPreviewDao;
  private final RecordDao recordPublishDao;
  private final MorphiaDatastoreProviderImpl morphiaDatastoreProvider;
  private final WorkflowExecutionDao workflowExecutionDao;

  public ApplicationRunner(RecordDao recordPreviewDao,
      RecordDao recordPublishDao, MorphiaDatastoreProviderImpl morphiaDatastoreProvider,
      WorkflowExecutionDao workflowExecutionDao) {
    this.recordPreviewDao = recordPreviewDao;
    this.recordPublishDao = recordPublishDao;
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
    this.workflowExecutionDao = workflowExecutionDao;
  }

  public void run() {
    LOGGER.info("Starting Application");
    final List<String> allDatasetIds = getAllDatasetIds();
    final Map<String, RecordTotals> totalRecordsPerDatasetId = getTotalRecordsPerDatasetId(allDatasetIds);
    updateRecordTotals(totalRecordsPerDatasetId);
  }

  private void updateRecordTotals(Map<String, RecordTotals> totalRecordsPerDatasetId) {
    LOGGER.info("Updating record total for latest preview/publish plugins");
    if (dryRun) {
      LOGGER.info("Dry run is ENABLED! Updates WILL NOT be performed in the database");
    } else {
      LOGGER.info("Dry run is DISABLED! Updates WILL be performed in the database");
    }
    int counter = 0;
    for (Entry<String, RecordTotals> entry : totalRecordsPerDatasetId.entrySet()) {
      if (entry.getValue().getPreviewTotal() != 0) {
        updateRecordTotalsForIndexPlugin(entry.getKey(), entry.getValue().getPreviewTotal(), ExecutablePluginType.PREVIEW);
      }
      if (entry.getValue().getPublishTotal() != 0) {
        updateRecordTotalsForIndexPlugin(entry.getKey(), entry.getValue().getPublishTotal(), ExecutablePluginType.PUBLISH);
      }
      counter++;
      if (counter % 100 == 0) {
        LOGGER.info("Processed: {}", counter);
      }
    }
  }

  private void updateRecordTotalsForIndexPlugin(String datasetId, Long recordsTotal, ExecutablePluginType pluginType) {
    final PluginWithExecutionId<ExecutablePlugin> latestSuccessfulExecutablePlugin = workflowExecutionDao.getLatestSuccessfulExecutablePlugin(
        datasetId, EnumSet.of(pluginType), false);
    final WorkflowExecution workflowExecution = workflowExecutionDao.getByExternalTaskId(
        Long.parseLong(latestSuccessfulExecutablePlugin.getPlugin().getExternalTaskId()));
    final Optional<AbstractMetisPlugin> metisPluginWithType = workflowExecution
        .getMetisPluginWithType(latestSuccessfulExecutablePlugin.getPlugin().getPluginType());
    metisPluginWithType.ifPresent(
        abstractMetisPlugin -> {
          AbstractExecutablePlugin<?> executablePlugin = ((AbstractExecutablePlugin<?>) abstractMetisPlugin);
          LOGGER.info("Before update - datasetId: {}, workflowExecution: {}, totalRecords: {}", datasetId,
              workflowExecution.getId(), executablePlugin.getExecutionProgress().getTotalDatabaseRecords());
          executablePlugin.getExecutionProgress().setTotalDatabaseRecords(recordsTotal.intValue());
          LOGGER.info("After update - datasetId: {}, workflowExecution: {}, totalRecords: {}", datasetId,
              workflowExecution.getId(), executablePlugin.getExecutionProgress().getTotalDatabaseRecords());
        });
    if (!dryRun) {
      workflowExecutionDao.update(workflowExecution);
    }
  }

  private Query<FullBeanImpl> createPrefixDatasetIdMongoQuery(String datasetId, RecordDao recordPreviewDao) {
    final Pattern pattern = Pattern.compile("^" + Pattern.quote(getRecordIdPrefix(datasetId)));
    final Query<FullBeanImpl> query = recordPreviewDao.getDatastore().find(FullBeanImpl.class);
    query.filter(Filters.regex(ABOUT_FIELD).pattern(pattern));
    return query;
  }

  private static String getRecordIdPrefix(String datasetId) {
    return "/" + datasetId + "/";
  }

  public List<String> getAllDatasetIds() {
    LOGGER.info("Collecting all dataset ids");
    Query<Dataset> query = morphiaDatastoreProvider.getDatastore().find(Dataset.class);
    final List<Dataset> datasets = MorphiaUtils.getListOfQueryRetryable(query);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

  public Map<String, RecordTotals> getTotalRecordsPerDatasetId(List<String> datasetIds) {
    LOGGER.info("Collecting all total records preview/publish per dataset id");
    return datasetIds.stream().collect(Collectors.toMap(Function.identity(),
        datasetId -> new RecordTotals(createPrefixDatasetIdMongoQuery(datasetId, recordPreviewDao).count(),
            createPrefixDatasetIdMongoQuery(datasetId, recordPublishDao).count())));
  }

  private static class RecordTotals {

    private final Long previewTotal;
    private final Long publishTotal;

    public RecordTotals(Long previewTotal, Long publishTotal) {
      this.previewTotal = previewTotal;
      this.publishTotal = publishTotal;
    }

    public Long getPreviewTotal() {
      return previewTotal;
    }

    public Long getPublishTotal() {
      return publishTotal;
    }
  }
}
