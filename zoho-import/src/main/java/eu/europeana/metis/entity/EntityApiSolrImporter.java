package eu.europeana.metis.entity;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityApiSolrImporter {

  String solrUrl;
  SolrClient solrClient;
  static final Logger LOGGER = LoggerFactory.getLogger(EntityApiSolrImporter.class);
  
  
  public EntityApiSolrImporter(String solrUrl){
    this.solrUrl = solrUrl;
    solrClient = (new HttpSolrClient.Builder(solrUrl)).build();
  }

  public String getSolrUrl() {
    return solrUrl;
  }
  
  public void delete(String entityId, boolean commit) throws SolrServerException, IOException{
      getSolrClient().deleteByQuery("id:\"" + entityId + "\"");
      LOGGER.debug("deleted entity with id {}", entityId);
      if(commit){
        getSolrClient().commit();
        LOGGER.debug("commited delete operation!");
      }
  }

  public void commit() throws SolrServerException, IOException{
      getSolrClient().commit();
      LOGGER.debug("commited previously submitted operations!");    
  }
  
  public void add(File xmlFile, boolean commit) throws SolrServerException, IOException{
    SolrQuery query = new SolrQuery();
    query.set(CommonParams.QT, "/update");
    query.set("document-type", "solr");
    String doc = FileUtils.readFileToString(xmlFile, "UTF-8");
    query.set("document", doc);
    
    getSolrClient().query(query, METHOD.POST);
    if(commit){
      getSolrClient().commit();
      LOGGER.debug("commited add document operation! File: {}", xmlFile.getAbsolutePath());
    }
  }
  
  
  public boolean exists(String entityId) throws SolrServerException, IOException{
      SolrQuery query = new SolrQuery("id:\"" + entityId + "\"");
      query.setFields("id");
      QueryResponse resp = getSolrClient().query(query);
      SolrDocumentList docs = resp.getResults();
      
      return (docs!= null && !docs.isEmpty());   
  }
  
  public SolrClient getSolrClient() {
    return solrClient;
  }
}
