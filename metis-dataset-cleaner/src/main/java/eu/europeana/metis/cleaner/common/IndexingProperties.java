package eu.europeana.metis.cleaner.common;

public class IndexingProperties {

  private String mongoInstances;
  private int mongoPortNumber;
  private String mongoDbName;
  private String mongoRedirectsDbName;
  private String mongoUsername;
  private String mongoPassword;
  private String mongoAuthDb;
  private String mongoUseSSL;
  private String mongoReadPreference;
  private String mongoApplicationName;
  private int mongoPoolSize;

  private String solrInstances;

  private String zookeeperInstances;
  private int zookeeperPortNumber;
  private String zookeeperChroot;
  private String zookeeperDefaultCollection;

  @Override
  public String toString() {
    return "IndexingProperties{" +
        "mongoInstances='" + mongoInstances + '\'' +
        ", mongoPortNumber=" + mongoPortNumber +
        ", mongoDbName='" + mongoDbName + '\'' +
        ", mongoRedirectsDbName='" + mongoRedirectsDbName + '\'' +
        ", mongoUsername='" + mongoUsername + '\'' +
        ", mongoPassword='" + mongoPassword + '\'' +
        ", mongoAuthDb='" + mongoAuthDb + '\'' +
        ", mongoUseSSL='" + mongoUseSSL + '\'' +
        ", mongoReadPreference='" + mongoReadPreference + '\'' +
        ", mongoApplicationName='" + mongoApplicationName + '\'' +
        ", mongoPoolSize=" + mongoPoolSize +
        ", solrInstances='" + solrInstances + '\'' +
        ", zookeeperInstances='" + zookeeperInstances + '\'' +
        ", zookeeperPortNumber=" + zookeeperPortNumber +
        ", zookeeperChroot='" + zookeeperChroot + '\'' +
        ", zookeeperDefaultCollection='" + zookeeperDefaultCollection + '\'' +
        '}';
  }

  public String getMongoInstances() {
    return mongoInstances;
  }

  public void setMongoInstances(String mongoInstances) {
    this.mongoInstances = mongoInstances;
  }

  public int getMongoPortNumber() {
    return mongoPortNumber;
  }

  public void setMongoPortNumber(int mongoPortNumber) {
    this.mongoPortNumber = mongoPortNumber;
  }

  public String getMongoDbName() {
    return mongoDbName;
  }

  public void setMongoDbName(String mongoDbName) {
    this.mongoDbName = mongoDbName;
  }

  public String getMongoRedirectsDbName() {
    return mongoRedirectsDbName;
  }

  public void setMongoRedirectsDbName(String mongoRedirectsDbName) {
    this.mongoRedirectsDbName = mongoRedirectsDbName;
  }

  public String getMongoUsername() {
    return mongoUsername;
  }

  public void setMongoUsername(String mongoUsername) {
    this.mongoUsername = mongoUsername;
  }

  public String getMongoPassword() {
    return mongoPassword;
  }

  public void setMongoPassword(String mongoPassword) {
    this.mongoPassword = mongoPassword;
  }

  public String getMongoAuthDb() {
    return mongoAuthDb;
  }

  public void setMongoAuthDb(String mongoAuthDb) {
    this.mongoAuthDb = mongoAuthDb;
  }

  public String getMongoUseSSL() {
    return mongoUseSSL;
  }

  public void setMongoUseSSL(String mongoUseSSL) {
    this.mongoUseSSL = mongoUseSSL;
  }

  public String getMongoReadPreference() {
    return mongoReadPreference;
  }

  public void setMongoReadPreference(String mongoReadPreference) {
    this.mongoReadPreference = mongoReadPreference;
  }

  public String getMongoApplicationName() {
    return mongoApplicationName;
  }

  public void setMongoApplicationName(String mongoApplicationName) {
    this.mongoApplicationName = mongoApplicationName;
  }

  public int getMongoPoolSize() {
    return mongoPoolSize;
  }

  public void setMongoPoolSize(int mongoPoolSize) {
    this.mongoPoolSize = mongoPoolSize;
  }

  public String getSolrInstances() {
    return solrInstances;
  }

  public void setSolrInstances(String solrInstances) {
    this.solrInstances = solrInstances;
  }

  public String getZookeeperInstances() {
    return zookeeperInstances;
  }

  public void setZookeeperInstances(String zookeeperInstances) {
    this.zookeeperInstances = zookeeperInstances;
  }

  public int getZookeeperPortNumber() {
    return zookeeperPortNumber;
  }

  public void setZookeeperPortNumber(int zookeeperPortNumber) {
    this.zookeeperPortNumber = zookeeperPortNumber;
  }

  public String getZookeeperChroot() {
    return zookeeperChroot;
  }

  public void setZookeeperChroot(String zookeeperChroot) {
    this.zookeeperChroot = zookeeperChroot;
  }

  public String getZookeeperDefaultCollection() {
    return zookeeperDefaultCollection;
  }

  public void setZookeeperDefaultCollection(String zookeeperDefaultCollection) {
    this.zookeeperDefaultCollection = zookeeperDefaultCollection;
  }
}
