package eu.europeana.metis.endpoints.mapper.utilities;

import java.util.List;
import java.util.Set;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.Workflow;

public class ExtendedWorkflowDao extends eu.europeana.metis.core.dao.WorkflowDao {

  private MorphiaDatastoreProvider morphiaDatastoreProvider;

  public ExtendedWorkflowDao(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    super(morphiaDatastoreProvider);
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
  }

  public List<Workflow> getAllWorkflows(Set<String> datasetIds) {
    return morphiaDatastoreProvider.getDatastore().find(Workflow.class).field("datasetId")
        .in(datasetIds).asList();
  }
}
