package eu.europeana.metis.depublishing.config;

/**
 * Mode of execution of the script.
 *
 */
public enum Mode {
  DEFAULT, REPROCESS_ALL_FAILED;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Wrong Mode supplied");
  }
}
