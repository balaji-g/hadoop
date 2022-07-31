package org.apache.hadoop.fs.s3a.cache;
  
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpHost;
import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;

public final class CacheClient {


    private static final Logger LOG =
        LoggerFactory.getLogger(CacheClient.class);


    private HttpClient httpClient;
    private HttpHost httpHost;

    public CacheClient() {
        this.httpClient = CacheConnection.getInstance().createHttpClient();
        httpHost = new HttpHost("127.0.0.1", 9999);
    }

    public String[] getKey(final String key) {
        org.apache.http.HttpResponse response = null;
        String location = null;
        String size = null;
        try {
            HttpRequestBase baseReq = new HttpGet("/");
            baseReq.addHeader("x-nv-key", key);
            response = httpClient.execute(httpHost, baseReq);
            int status = response.getStatusLine().getStatusCode();
            if (status == 200) {
                final Header hdr = response.getFirstHeader("x-nv-value");
                final Header hdr2 = response.getFirstHeader("x-nv-size");
                if (hdr != null && hdr2 != null) {
                    location = hdr.getValue();
                    size = hdr2.getValue();
                    LOG.debug("Found key: {},{},{}", key, location, size);
                    return new String[] {location, size};
                } else {
                    LOG.error("Value not found for key {}", key);
                }
            }
        } catch (IOException ioe) {
            LOG.error("Caught {} when executing getKey for {}", ioe, key);
        } finally {
            try {
                if (response != null
                    && (response.getEntity() != null)
                    && (response.getEntity().getContent() != null)) {
                    response.getEntity().getContent().close();
                }
            } catch (Throwable t) {
                LOG.info("Caught", t);
            }
        }
        return null;
    }

    public void putKey(final String key,
                         final String value,
                         final int size) {
        org.apache.http.HttpResponse response = null;
        try {
            HttpRequestBase baseReq = new HttpPut("/");
            baseReq.addHeader("x-nv-key", key);
            baseReq.addHeader("x-nv-value", value);
            baseReq.addHeader("x-nv-size", String.valueOf(size));
            response = httpClient.execute(httpHost, baseReq);
            int status = response.getStatusLine().getStatusCode();
            if (status == 200) {
                LOG.debug("Adding key: {},{},{}", key, value, size);
            }
        } catch (IOException ioe) {
            LOG.error("Caught {} when executing putKey for {}", ioe, key);
        } finally {
            try {
                if (response != null
                    && (response.getEntity() != null)
                    && (response.getEntity().getContent() != null)) {
                    response.getEntity().getContent().close();
                }
            } catch (Throwable t) {
                LOG.info("Caught", t);
            }
        }
    }

}
