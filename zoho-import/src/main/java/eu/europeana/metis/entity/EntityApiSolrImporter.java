package eu.europeana.metis.entity;

import java.io.File;
import java.io.IOException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MimeTypeUtils;

public class EntityApiSolrImporter {

  String solrUrl;
  SolrClient solrClient;
  static final Logger LOGGER = LoggerFactory.getLogger(EntityApiSolrImporter.class);


  public EntityApiSolrImporter(String solrUrl) {
    this.solrUrl = solrUrl;
    solrClient = (new HttpSolrClient.Builder(solrUrl)).build();
  }

  public String getSolrUrl() {
    return solrUrl;
  }

  public void delete(String entityId, boolean commit) throws SolrServerException, IOException {
    getSolrClient().deleteByQuery("id:\"" + entityId + "\"");
    LOGGER.debug("deleted entity with id {}", entityId);
    if (commit) {
      getSolrClient().commit();
      LOGGER.debug("commited delete operation!");
    }
  }

  public void commit() throws SolrServerException, IOException {
    getSolrClient().commit();
    LOGGER.debug("commited previously submitted operations!");
  }

  /**
   * Add entity using content stream update request.
   * 
   * @param xmlFile The XML content to add
   * @param commit Boolean commit parameter - commit operation executed if True
   * @throws SolrServerException
   * @throws IOException
   */
  public void add(File xmlFile, boolean commit) throws SolrServerException, IOException {
    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update");
    req.addFile(xmlFile, MimeTypeUtils.APPLICATION_XML_VALUE);
    NamedList<Object> result = getSolrClient().request(req);
    LOGGER.debug("add operation result: " + result);
    if (commit) {
      getSolrClient().commit();
      LOGGER.debug("commited add document operation! File: {}", xmlFile.getAbsolutePath());
    }
  }


  public boolean exists(String entityId) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery("id:\"" + entityId + "\"");
    query.setFields("id");
    QueryResponse resp = getSolrClient().query(query);
    SolrDocumentList docs = resp.getResults();

    return (docs != null && !docs.isEmpty());
  }

  public SolrClient getSolrClient() {
    return solrClient;
  }
}
