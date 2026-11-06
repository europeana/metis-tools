package eu.europeana.metis.migration.counter.config;


public enum Mode {
  PRE_PROCESS, DEFAULT, REPROCESS_ALL_FAILED,  POST_PROCESS;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Wrong Mode supplied");
  }
}
