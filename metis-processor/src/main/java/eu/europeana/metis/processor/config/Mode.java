package eu.europeana.metis.processor.config;


public enum Mode {
  DEFAULT, REPROCESS_ALL_FAILED, DRY_RUN;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Wrong Mode supplied");
  }
}
