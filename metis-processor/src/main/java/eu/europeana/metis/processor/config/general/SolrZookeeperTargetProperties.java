package eu.europeana.metis.processor.config.general;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Class that is used to read all configuration properties for the application.
 * <p>
 * It uses {@link PropertySource} to identify the properties on application startup
 * </p>
 */
@Component
public class SolrZookeeperTargetProperties {

    @Value("${solr.target.hosts}")
    private String[] solrTargetHosts;
    @Value("${zookeeper.target.hosts}")
    private String[] zookeeperTargetHosts;
    @Value("${zookeeper.target.port}")
    private int[] zookeeperTargetPorts;
    @Value("${zookeeper.target.chroot}")
    private String zookeeperTargetChroot;
    @Value("${zookeeper.target.defaultCollection}")
    private String zookeeperTargetDefaultCollection;

    public String[] getSolrTargetHosts() {
        return solrTargetHosts;
    }

    public String[] getZookeeperTargetHosts() {
        return zookeeperTargetHosts;
    }

    public int[] getZookeeperTargetPorts() {
        return zookeeperTargetPorts;
    }

    public String getZookeeperTargetChroot() {
        return zookeeperTargetChroot;
    }

    public String getZookeeperTargetDefaultCollection() {
        return zookeeperTargetDefaultCollection;
    }
}
