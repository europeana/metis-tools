package eu.europeana.metis.zoho.exception;

public class SolrDocGenerationException extends Exception {

  private static final long serialVersionUID = 1714819334377803796L;

  public SolrDocGenerationException(String message, Throwable th){
    super(message, th);
  }
  
  public SolrDocGenerationException(String message){
    super(message, null);
  }
}
