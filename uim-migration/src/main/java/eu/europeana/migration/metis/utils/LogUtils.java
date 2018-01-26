package eu.europeana.migration.metis.utils;

public final class LogUtils {

  private LogUtils() {}

  public static void logInfoMessage(String message) {
    System.out.println(message);
  }

  public static void logException(Throwable exception) {
    System.out.println("An exception occurred:");
    exception.printStackTrace();
  }

}
