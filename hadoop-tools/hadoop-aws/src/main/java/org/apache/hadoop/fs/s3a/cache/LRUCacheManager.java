package org.apache.hadoop.fs.s3a.cache;
  

public final class LRUCacheManager {


    public static LRUCacheManager getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private static class SingletonHolder {
        private static final LRUCacheManager INSTANCE = new LRUCacheManager();
    }


    private LRUCacheManager() {
    }


}
