package com.tribbloids.spookystuff.utils.http;

import org.apache.http.conn.ssl.X509HostnameVerifier;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.security.cert.X509Certificate;

/**
 * WARNING: keep until Spark get rid of httpclient 4.3
 */
public class InsecureHostnameVerifier implements X509HostnameVerifier {

    public boolean verify(String arg0, SSLSession arg1) {
        return true;
        // mark everything as verified
    }

    public void verify(String host, SSLSocket ssl) throws IOException {

    }

    public void verify(String host, X509Certificate cert) throws SSLException {

    }

    public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {

    }
}