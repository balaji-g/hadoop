package org.apache.hadoop.fs.s3a.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LRUCache {

    class CacheBlock {

        CacheBlock prev;
        CacheBlock next;
        long size = 0L;
        String location;
        String key;

        public CacheBlock(final long _sz, final String _loc, final String _k) {
            this.size = _sz;
            this.location = _loc;
            this.key = _k;
        }

    }

	CacheBlock head;
	CacheBlock tail;
    long capacity = 0L;
    long currentSize = 0L;
    String location;
	ConcurrentHashMap<String, CacheBlock> map = null;
    
    private static final Logger LOG =
        LoggerFactory.getLogger(LRUCache.class);
	

    public LRUCache(final long _c, final String _loc) {
        this.capacity = _c;
        this.location = _loc;
        map = new ConcurrentHashMap<String, CacheBlock>();
    }

    public S3ObjectInputStream get(final String key) {
        CacheBlock blk = map.get(key);
        if (blk == null) {
            return null;
        }
        
        //Move to tail;
        removeBlock(blk);
        offerBlock(blk);
        try {
            LOG.info("Found key {}, in cache.", key);
            return new S3ObjectInputStream(new FileInputStream(new File(blk.location)), null);
        } catch (FileNotFoundException e) {
            LOG.error("Caught {}, {}", e, blk.location);
        }

        return null;
    }


    public S3ObjectInputStream put(final String key, final S3ObjectInputStream is, final long size) {

        //If adding current size would go beyond capacity. Start deleting
        while (currentSize + size > capacity) {
            map.remove(head.key);
            currentSize -= head.size;
            removeBlock(head);
            deleteFile(head.location);
            LOG.info("Removing key {}, from cache.", head.key);
        }

        //write file
        String fileLoc = location + key;
        long read = writeFile(fileLoc, is, size);
        if (read != size) {
            LOG.error("Not enough bytes read {}, {}, {}", read, size, key);
            return null;
        }
        
        //add to tail
        CacheBlock blk = new CacheBlock(size, fileLoc, key);
        offerBlock(blk);
        map.put(key, blk);
        currentSize += size;
        LOG.info("Adding key {}, to cache.", key);
        
        try {
            return new S3ObjectInputStream(new FileInputStream(new File(fileLoc)), null);
        } catch (FileNotFoundException e) {
            LOG.error("Caught {}, {}", e, fileLoc);
        }
        return null;
    }

    private synchronized void removeBlock(final CacheBlock n) {
        if (n.prev != null) {
            n.prev.next = n.next;
        } else {
            head = n.next;
        }

        if (n.next != null) {
            n.next.prev = n.prev;
        } else {
            tail = n.prev;
        }
    }

    private synchronized void offerBlock(final CacheBlock n) {
        if (tail != null) {
            tail.next = n;
        }
        
        n.prev = tail;
        n.next = null;
        tail = n;
        
        if (head == null) {
            head = tail;
        }
    }
    
    private void deleteFile(final String key) {
        try {
            File f = new File(location + key);
            f.delete();
        } catch (Exception e) {
            LOG.warn("Caught exception {} when deleting {}", e, location+key);
        }
    }
    
    private long writeFile(final String fileLoc, final S3ObjectInputStream is, final long size) {
        File f = null;
        FileOutputStream fos = null;
        FileChannel fc = null;
        ReadableByteChannel rc = null;
        long bytesRead = 0L;
        boolean fail = false;
        
        try {
            f = new File(fileLoc);
            fos = new FileOutputStream(f);
            fc = fos.getChannel();
            rc = Channels.newChannel(is);

            bytesRead = fc.transferFrom(rc, 0, size);
        } catch (Exception e) {
            fail = true;
            LOG.error("Caught exception {} when writting to {}", e, fileLoc);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (Exception e) {
                    LOG.error("Caught exception {}", e);
                }
            }
            
            //Don't leave 0 byte files on disk.
            if (fail && (f != null)) {
                deleteFile(fileLoc);
            }
        }
        return bytesRead;
    }
}

