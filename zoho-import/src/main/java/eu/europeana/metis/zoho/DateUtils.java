package eu.europeana.metis.zoho;

import java.text.ParseException;
import java.util.Date;

public class DateUtils {

	
  public static Date parseDate(String dateString) {
    try {
    	return ZohoConstants.ZOHO_DATE_FORMATTER.parse(dateString);
    } catch (ParseException e) {
      String message = "Provided value ("+ dateString +") does not match expected date format: " + ZohoConstants.ZOHO_TIME_FORMAT;
      throw new IllegalArgumentException(message, e);
    }
  }
}
