package eu.europeana.metis.zoho.python;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.io.IOUtils;
import eu.europeana.metis.zoho.exception.SolrDocGenerationException;

public class SolrDocGeneratorPy {

  private final String python;
  private final String scriptPath;
  private final String pythonPath;
  private final String workingDir;
  private StringBuilder cmdBuilder;
  private static final String OUTPUT_MARKER = "generated:";
  
  public SolrDocGeneratorPy(String python, String pythonPath, String scriptPath, String workingDir) {
    this.python = python;
    this.pythonPath = pythonPath;
    this.scriptPath = scriptPath;
    this.workingDir = workingDir;
    init();
  }

  private void init() {
    cmdBuilder = new StringBuilder();
    cmdBuilder.append(python);
    cmdBuilder.append(" -u ");
    cmdBuilder.append(scriptPath);
    cmdBuilder.append(" ");
  }

  public File generateSolrDoc(String entityId) throws SolrDocGenerationException {
    Process process;
    BufferedReader out = null;
    BufferedReader error = null;
    File generatedFile;
    String command = null;
    
    try {
      cmdBuilder.append(entityId);
      command = cmdBuilder.toString();
      File workDir = new File(workingDir);
      String[] envp = new String[]{"PYTHONPATH=" + pythonPath}; 
      process = Runtime.getRuntime().exec(command,envp, workDir);

      out = new BufferedReader(new InputStreamReader(process.getInputStream()));
      error = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      
      String filePath = getResult(out, error);
      generatedFile = new File(filePath);
      
      if(!generatedFile.exists()){
        throw new SolrDocGenerationException("Cannot access the python generated document: " + generatedFile.getAbsolutePath());
      }
      
    } catch (IOException e) {
      throw new SolrDocGenerationException("Cannot execute python command: " + command, e); 
    } finally {
      closeReaders(out, error);
    }
    
    return generatedFile;
  }

  protected String getResult(BufferedReader out, BufferedReader error) throws SolrDocGenerationException {
    //check expected output first
    String line;
    
    try {
      //read output
      while((line = out.readLine()) != null){
        line = line.trim();
        if(line.startsWith(OUTPUT_MARKER)){
          //return the name of the generated file
          return line.substring(OUTPUT_MARKER.length()).trim();
        }
      }
      
      //no outputMarker detected, check error stream
      String errorMessage = IOUtils.toString(error);
      throw new SolrDocGenerationException("Python code error: " + errorMessage);
    } catch (IOException e) {
      throw new SolrDocGenerationException("Cannot access output of Python Process. ", e);
    }
  }

  private void closeReaders(BufferedReader out, BufferedReader error) {
    if(out != null){
      try {
        out.close();
      } catch (Exception e) {
        //nothing to do reader is already closed
      }
    }
      
    if(error != null){
      try{
        error.close();
      } catch (Exception e) {
      //nothing to do reader is already closed
      } 
    }
  }
  
}
