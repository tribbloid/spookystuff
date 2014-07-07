/*
 * Copyright (c) 2002-2013 Gargoyle Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tribbloid.spookystuff.utils;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A completely insecure (yet very easy to use) x509 trust manager. This manager trusts all servers
 * and all clients, regardless of credentials or lack thereof.
 *
 * @version $Revision: 8393 $
 * @author Daniel Gredler
 * @author Marc Guillemot
 */
public class InsecureTrustManager implements X509TrustManager {
    private final Set<X509Certificate> acceptedIssuers_ = new HashSet<X509Certificate>();

    /**
     * {@inheritDoc}
     */
    public void checkClientTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        // Everyone is trusted!
        acceptedIssuers_.addAll(Arrays.asList(chain));
    }

    /**
     * {@inheritDoc}
     */
    public void checkServerTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
        // Everyone is trusted!
        acceptedIssuers_.addAll(Arrays.asList(chain));
    }

    /**
     * {@inheritDoc}
     */
    public X509Certificate[] getAcceptedIssuers() {
        // it seems to be OK for Java <= 6 to return an empty array but not for Java 7 (at least 1.7.0_04-b20):
        // requesting an URL with a valid certificate (working without WebClient.setUseInsecureSSL(true)) throws a
        //  javax.net.ssl.SSLPeerUnverifiedException: peer not authenticated
        // when the array returned here is empty
        if (acceptedIssuers_.isEmpty()) {
            return new X509Certificate[0];
        }
        return acceptedIssuers_.toArray(new X509Certificate[acceptedIssuers_.size()]);
    }
}
