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

    public String getImageEnhancerEndpoint() {
        return imageEnhancerEndpoint;
    }

    public int getImageEnhancerConnectTimeout() {
        return imageEnhancerConnectTimeout;
    }

    public int getImageEnhancerReadTimeout() {
        return imageEnhancerReadTimeout;
    }
}
