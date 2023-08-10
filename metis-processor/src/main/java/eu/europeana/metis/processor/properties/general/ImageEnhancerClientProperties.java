package eu.europeana.metis.processor.properties.general;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ImageEnhancerClientProperties {

    @Value("${image.enhancer.endpoint}")
    private String imageEnhancerEndpoint;
    @Value("${image.enhancer.connect.timeout}")
    private int imageEnhancerConnectTimeout;
    @Value("${image.enhancer.read.timeout}")
    private int imageEnhancerReadTimeout;
    @Value("${image.enhancer.script.path}")
    private String imageEnhancerScriptPath;

    public String getImageEnhancerEndpoint() {
        return imageEnhancerEndpoint;
    }

    public int getImageEnhancerConnectTimeout() {
        return imageEnhancerConnectTimeout;
    }

    public int getImageEnhancerReadTimeout() {
        return imageEnhancerReadTimeout;
    }

    public String getImageEnhancerScriptPath() {
        return imageEnhancerScriptPath;
    }
}
