package org.tribbloid.spookystuff.utils;

import org.apache.http.ProtocolException;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.net.*;

/**
 * Created by peng on 2/4/15.
 */
public class UrlEncodingRedirectStrategy extends LaxRedirectStrategy {

//  @Override
  protected URI createLocationURI(final String location) throws ProtocolException {
    // One can try to rewrite malformed redirect locations
    //at this point
    try {
      URL url = new URL(location);

      return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ProtocolException("malformed location", e);
    }
  }
}