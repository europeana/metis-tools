package eu.europeana.metis.technical.metadata.generation.utilities;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import org.mongodb.morphia.Datastore;
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
  private final Datastore datastore;

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final DateFormat dateFormat;

  static {
    dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public ExecutorManager(Datastore datastore) {
    this.datastore = datastore;
  }

  public void startTechnicalMetadataGeneration() {

  }
}
