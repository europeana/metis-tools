package eu.europeana.metis.published.records.harvest;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.metis.core.common.DaoFieldNames;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dao.WorkflowExecutionDao.ResultList;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.IndexToPublishPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.published.records.harvest.utils.Application;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  // TODO TEMP
  private static final String INPUT = """
      /1227/europeana_fashion_0
      /1227/europeana_fashion_1
      /1227/europeana_fashion_10
      /1227/europeana_fashion_11
      /1227/europeana_fashion_12
      /1227/europeana_fashion_13
      /1227/europeana_fashion_14
      /1227/europeana_fashion_15
      /1227/europeana_fashion_16
      /1227/europeana_fashion_2
      /1227/europeana_fashion_3
      /1227/europeana_fashion_4
      /1227/europeana_fashion_5
      /1227/europeana_fashion_6
      /1227/europeana_fashion_7
      /1227/europeana_fashion_8
      /1227/europeana_fashion_9
      /1517/urn___en_home_1_261e176f_9890_4136_8edf_c554bd4b7ee7
      /1517/urn___en_home_1_2dcba0a1_d64b_4a15_854a_6e513f8ca345
      /1517/urn___en_home_1_d4149df4_ae68_4468_83cf_a8518ead9c0c
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_1
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_10
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_11
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_12
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_13
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_14
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_15
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_2
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_3
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_4
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_5
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_6
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_7
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_8
      /654/_providedCHO_https___hdl_handle_net_10622_COLL00415_https___hdl_handle_net_10622_COLL00415_9
      /9200309/BibliographicResource_3000093756777_source
      /9200309/BibliographicResource_3000093756781_source
      /9200309/BibliographicResource_3000093756782_source
      /9200309/BibliographicResource_3000093756772_source
      /9200309/BibliographicResource_3000093756774_source
      /9200309/BibliographicResource_3000093756780_source
      /9200309/BibliographicResource_3000093756784_source
      /9200309/BibliographicResource_3000093756776_source
      /9200309/BibliographicResource_3000093756778_source
      /9200309/BibliographicResource_3000093756775_source
      /9200309/BibliographicResource_3000093756779_source
      /9200309/BibliographicResource_3000093756773_source
      /9200309/BibliographicResource_3000093756785_source
      /9200309/BibliographicResource_3000093756787_source
      /111/https___hispana_mcu_es_lod_oai_jable_ulpgc_es_pandora_0006068939_ent0
      """;

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {
    try (final Application application = Application.initialize()) {
      final Map<String, Set<String>> records = readInput();
      final Map<String, List<Revision>> revisions = getLatestPublishRevisions(records.keySet(),
          application);
      System.out.println(revisions.size());
    }
  }

  /**
   * @return All record IDs, separated by dataset ID.
   * @throws IOException IO issues.
   */
  private static Map<String, Set<String>> readInput() throws IOException {
    final Map<String, Set<String>> result = new HashMap<>();
    try (final StringReader input = new StringReader(INPUT); // TODO Make FileReader
        final BufferedReader reader = new BufferedReader(input)) {
      while (true) {
        final String line = reader.readLine();
        if (line == null) {
          break;
        }
        if (line.isBlank()) {
          continue;
        }
        final String trimmedLine = line.trim();
        final int datasetEnd = trimmedLine.indexOf('/', 1);
        if (datasetEnd <= 1 || datasetEnd != trimmedLine.lastIndexOf('/')) {
          LOGGER.warn("Ignoring invalid input: {}", trimmedLine);
          continue;
        }
        result.computeIfAbsent(trimmedLine.substring(1, datasetEnd), key -> new HashSet<>())
            .add(trimmedLine);
      }
    }
    return result;
  }

  /**
   * @param datasets    The datasets for which to get the revisions.
   * @param application This application.
   * @return For each dataset, the latest revisions (in reverse order). This is the set of index to
   * publish revisions since (and including) that latest full processing publish.
   */
  private static Map<String, List<Revision>> getLatestPublishRevisions(Set<String> datasets,
      Application application) {
    final Map<String, List<Revision>> result = new HashMap<>();
    final WorkflowExecutionDao workflowExecutionDao = new WorkflowExecutionDao(
        application.getDatastoreProvider());
    for (final String dataset : datasets) {
      result.put(dataset, getLatestPublishRevisions(dataset, workflowExecutionDao, application));
    }
    return result;
  }

  /**
   * @param dataset              The dataset for which to get the revisions.
   * @param workflowExecutionDao The DAO to use.
   * @param application          This application.
   * @return For the dataset, the latest revisions (in reverse order). This is the set of index to
   * publish revisions since (and including) that latest full processing publish.
   */
  private static List<Revision> getLatestPublishRevisions(String dataset,
      WorkflowExecutionDao workflowExecutionDao, Application application) {
    final ResultList<WorkflowExecution> reverseOrderedExecutions = workflowExecutionDao
        .getAllWorkflowExecutions(Set.of(dataset), Set.of(WorkflowStatus.FINISHED),
            DaoFieldNames.CREATED_DATE, false, 0, null, true);
    final List<Revision> result = new ArrayList<>();
    for (WorkflowExecution workflowExecution : reverseOrderedExecutions.results()) {
      for (AbstractMetisPlugin<?> plugin : workflowExecution.getMetisPlugins().reversed()) {
        if (plugin.getPluginType() != PluginType.PUBLISH
            || !(plugin instanceof IndexToPublishPlugin publishPlugin)) {
          continue;
        }
        result.add(new Revision(plugin.getPluginType().name(),
            application.getProperties().ecloudProvider, plugin.getStartedDate(), false));
        if (!publishPlugin.getPluginMetadata().isIncrementalIndexing()) {
          return result;
        }
      }
    }
    LOGGER.error("No (full processing) plugin is found for dataset {}.", dataset);
    return result;
  }
}
