package eu.europeana.metis.finder.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"eu.europeana.metis.finder"})
public class MetisWebresourceFinderApplication {

	public static void main(String[] args) {
		SpringApplication.run(MetisWebresourceFinderApplication.class, args);
	}

}
