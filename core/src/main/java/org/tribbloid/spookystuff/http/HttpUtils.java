package org.tribbloid.spookystuff.http;

import oauth.signpost.basic.DefaultOAuthConsumer;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.signature.HmacSha1MessageSigner;
import oauth.signpost.signature.PlainTextMessageSigner;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by peng on 2/4/15.
 */
public class HttpUtils {

  private static URL dummyURL() {
    try {
      return new URL("http://www.dummy.com/");
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private static URL dummyURL = dummyURL();

  public static String uriStr(String s) throws URISyntaxException {
    return uri(s).toString();
  }

  public static URI uri(String s) throws URISyntaxException {

    //this solution is abandoned as it cannot handle question mark
//    URI uri;
//    try {
//      uri = new URI(s);
//    }
//    catch (URISyntaxException e) {
//      uri = new URI(URIUtil.encodePath(s));
//    }
//
//    return uri.normalize();

    try {
      return new URI(s);
    }
    catch (URISyntaxException e) {
      try {
        URL url = new URL(s);
        return new URI(url.getProtocol(), url.getAuthority(), url.getPath(), url.getQuery(), null);
      } catch (MalformedURLException ee) {
        URL url;
        try {
          url = new URL(dummyURL, s);
        } catch (MalformedURLException eee) {
          throw new RuntimeException(eee);
        }
        return new URI(null, null, url.getPath(), url.getQuery(), null); //this will generate a relative URI the string itself is relative
      }
    }
  }

  static PlainTextMessageSigner plainTextMessageSigner = new PlainTextMessageSigner();
  static HmacSha1MessageSigner hmacSha1MessageSigner = new HmacSha1MessageSigner();

  public static String OauthV2(
          String uri,
          String CONSUMER_KEY,
          String CONSUMER_SECRET,

          String ACCESS_TOKEN,
          String TOKEN_SECRET
          ) throws OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException {

    // create a consumer object and configure it with the access
    // token and token secret obtained from the service provider
    DefaultOAuthConsumer consumer = new DefaultOAuthConsumer(CONSUMER_KEY, CONSUMER_SECRET);

    consumer.setMessageSigner(hmacSha1MessageSigner);

    consumer.setSendEmptyTokens(true);
    consumer.setTokenWithSecret(ACCESS_TOKEN, TOKEN_SECRET);

    // sign the request
    String result =  consumer.sign(uri);
    return result;
  }
}