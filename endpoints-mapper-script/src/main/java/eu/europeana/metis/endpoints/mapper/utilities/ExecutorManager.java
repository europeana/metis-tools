package eu.europeana.metis.endpoints.mapper.utilities;

import eu.europeana.metis.core.dao.DatasetDao;
import eu.europeana.metis.core.dataset.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles execution of the modes and has methods to convert csv lines to {@link Dataset}
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-05-02
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);

  private final PropertiesHolder propertiesHolder;
  private final DatasetDao datasetDaoOriginal;
  private final DatasetDao datasetDaoTemporary;

  public ExecutorManager(PropertiesHolder propertiesHolder, DatasetDao datasetDaoOriginal, DatasetDao datasetDaoTemporary) {
    this.propertiesHolder = propertiesHolder;
    this.datasetDaoOriginal = datasetDaoOriginal;
    this.datasetDaoTemporary = datasetDaoTemporary;
  }

  public void createMode() {
  }

  public void deleteMode() {

  }

}
