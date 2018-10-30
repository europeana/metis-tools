package eu.europeana.metis.migration.results;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class provides functionality to parse the json that is sent by metis core when clicking on
 * the report of a task.
 */
public class LogToIdListParserMain {

  private static final String INPUT_FILE = "/home/jochen/Desktop/issue_404.json";

  public static void main(String[] args) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final Log log = mapper.readValue(new File(INPUT_FILE), Log.class);
    final List<String> recordIds = log.getErrors().stream().map(Error::getErrorDetails)
        .flatMap(List::stream).map(ErrorDetails::getIdentifier).sorted()
        .collect(Collectors.toList());
    recordIds.forEach(System.out::println);
  }


  private static class Log {

    private long id;
    private List<Error> errors;

    public long getId() {
      return id;
    }

    public List<Error> getErrors() {
      return errors;
    }
  }

  private static class Error {

    private String errorType;
    private String message;
    private int occurrences;
    private List<ErrorDetails> errorDetails;

    public String getErrorType() {
      return errorType;
    }

    public String getMessage() {
      return message;
    }

    public int getOccurrences() {
      return occurrences;
    }

    public List<ErrorDetails> getErrorDetails() {
      return errorDetails;
    }
  }

  private static class ErrorDetails {

    private String identifier;
    private String additionalInfo;

    public String getIdentifier() {
      return identifier;
    }

    public String getAdditionalInfo() {
      return additionalInfo;
    }
  }

}
