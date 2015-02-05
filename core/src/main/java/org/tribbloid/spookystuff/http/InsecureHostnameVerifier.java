package org.tribbloid.spookystuff.http;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class InsecureHostnameVerifier implements HostnameVerifier {

    public boolean verify(String arg0, SSLSession arg1) {
            return true;   // mark everything as verified
    }
}