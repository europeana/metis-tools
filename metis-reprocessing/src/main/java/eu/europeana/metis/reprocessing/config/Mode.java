package eu.europeana.metis.reprocessing.config;

/**
 * Mode of execution of the script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-19
 */
public enum Mode {
  DEFAULT, REPROCESS_ALL_FAILED, POST_PROCESS, POST_CLEAN, CLEAN;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Wrong Mode supplied");
  }
}
