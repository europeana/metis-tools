package eu.europeana.metis.finder.controllers;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import eu.europeana.metis.finder.domain.model.WebResource;
import eu.europeana.metis.finder.services.WebResourceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/webresource/")
@Tag(name = "Web Resource Controller")
public class WebResourceHashController {

  private final WebResourceService webResourceService;

  public WebResourceHashController(WebResourceService webResourceService) {
    this.webResourceService = webResourceService;
  }

  @Operation(summary = "Get webresource metadata hashcode", description = "Caulculates hascode base on record id and webresource")
  @ApiResponse(responseCode = "200")
  @ApiResponse(responseCode = "400")
  @GetMapping(value = "/hashcode", produces = APPLICATION_JSON_VALUE)
  public WebResource getAll(@RequestParam("webresourceId") String webResourceId,
      @RequestParam("recordId") String recordId) {
    return webResourceService.generateHashCode(webResourceId, recordId);
  }
}
