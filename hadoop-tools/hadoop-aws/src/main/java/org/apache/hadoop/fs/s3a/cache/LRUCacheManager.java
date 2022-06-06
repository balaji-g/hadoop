package org.apache.hadoop.fs.s3a.cache;
  
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public final class LRUCacheManager {


    private static final Logger LOG =
        LoggerFactory.getLogger(LRUCacheManager.class);

    private LRUCache cache = null;
    private long cacheSize = 0L;
    private boolean cacheEnabled = false;
    private String cachePath;

    public static LRUCacheManager getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private static class SingletonHolder {
        private static final LRUCacheManager INSTANCE = new LRUCacheManager();
    }


    private LRUCacheManager() {
    }

    public void initialize(final boolean enabled, final long size, final String path) {
        if (cache == null && enabled) {
            cache = new LRUCache(size, path);
        }
        this.cacheEnabled = enabled;
        this.cacheSize = size;
        this.cachePath = path;
    }

}
