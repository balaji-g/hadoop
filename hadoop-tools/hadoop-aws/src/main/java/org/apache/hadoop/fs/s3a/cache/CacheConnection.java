package org.apache.hadoop.fs.s3a.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;


public final class CacheConnection {


    private static final Logger LOG =
        LoggerFactory.getLogger(CacheConnection.class);

    private final PoolingHttpClientConnectionManager connMgr;
    private final RequestConfig reqConfig;
    private final SocketConfig soConfig;


   public static CacheConnection getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private static class SingletonHolder {
        private static final CacheConnection INSTANCE = new CacheConnection();
    }



    private CacheConnection() {
        final int nDEFAULTTIMEOUT = 10 * 1000;
        int nTimeoutMilliSeconds = nDEFAULTTIMEOUT;
        int nConnectionTimeoutMs = nDEFAULTTIMEOUT;
        boolean soReuseAddr = false;
        final int nSNDBUFFER = 524288;
        final int nRCVBUFFER = 524288;

        reqConfig = RequestConfig.custom()
                .setConnectTimeout(nConnectionTimeoutMs)
                .setSocketTimeout(nTimeoutMilliSeconds)
            //.setStaleConnectionCheckEnabled(true)  Deprecated and replaced by connMgr.setValidateAfterInactivity()
                .build();
        soConfig = SocketConfig.custom()
                .setTcpNoDelay(true)
                .setSoReuseAddress(soReuseAddr)
                .setSoKeepAlive(true)
                .setSndBufSize(nSNDBUFFER)
                .setRcvBufSize(nRCVBUFFER)
                .build();

        LOG.info("SocketConfiguration: " + soConfig);
        connMgr = new PoolingHttpClientConnectionManager();
        int nMaxConns = 100;
        connMgr.setMaxTotal(nMaxConns);
        int nMaxInUse = 100;
        connMgr.setDefaultMaxPerRoute(nMaxInUse);

        connMgr.setValidateAfterInactivity(nDEFAULTTIMEOUT);

        IdleConnectionReaper.getInstance().registerConnectionManager(connMgr);
    }

    public HttpClient createHttpClient() {
        return HttpClientBuilder.create()
                .setConnectionManager(connMgr)
                .setDefaultRequestConfig(reqConfig)
                .setDefaultSocketConfig(soConfig)
                .setUserAgent("nv-cache")
                .disableAutomaticRetries()
                .build();
    }

    public void shutdown() {
        connMgr.shutdown();
    }

}
