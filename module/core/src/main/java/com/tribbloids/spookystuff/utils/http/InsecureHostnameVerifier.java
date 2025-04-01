package com.tribbloids.spookystuff.utils.http;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

/**
 * WARNING: keep until Spark get rid of httpclient 4.3
 */
public class InsecureHostnameVerifier implements HostnameVerifier {

    public boolean verify(String arg0, SSLSession arg1) {
        return true;
        // mark everything as verified
    }
}