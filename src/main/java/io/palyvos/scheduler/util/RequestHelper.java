package io.palyvos.scheduler.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RequestHelper {

  private static final Logger LOG = LogManager.getLogger();

  public static String getContent(URIBuilder uriBuilder) {
    try {
      URI uri = uriBuilder.build();
      LOG.trace("Fetching {}", uri);
      return Request.Get(uri).execute().returnContent().toString();
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private RequestHelper() {
  }
}
