package eu.europeana.metis.technical.metadata.generation.model;

import eu.europeana.metis.core.workflow.HasMongoObjectId;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-05
 */
public interface DatasetFileStatus extends HasMongoObjectId {

  String getFileName();

  void setFileName(String fileName);

  int getLineReached();

  void setLineReached(int lineReached);

  boolean isEndOfFileReached();

  void setEndOfFileReached(boolean endOfFileReached);

}
