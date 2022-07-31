package org.apache.hadoop.fs.s3a.cache;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.util.concurrent.ThreadPoolExecutor;

/* Token */

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

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

    private class DeleteFiles implements Runnable {
        private List<String> files;
        private LRUCache cache;

        DeleteFiles(final List<String> _files, final LRUCache _cache) {
            this.files = _files;
            this.cache = _cache;
        }
        
        public void run() {
            cache.deleteFiles(files);
        }
    }

    private class WriteFile implements Runnable {
        private String key;
        private byte[] data;
        private int size;
        private LRUCache cache;

        WriteFile(final String _key, final byte[] _data, final int _size, final LRUCache _cache) {
            this.key = _key;
            this.data = _data;
            this.size = _size;
            this.cache = _cache;
        }
        
        public void run() {
            cache.writeFile(key, data, size);
        }
    }


	CacheBlock head;
	CacheBlock tail;
    long capacity = 0L;
    long currentSize = 0L;
    String location;
	HashMap<String, CacheBlock> map = null;
    ThreadPoolExecutor delTPE;
    ThreadPoolExecutor writeTPE;

    
    private static final Logger LOG =
        LoggerFactory.getLogger(LRUCache.class);
	

    public LRUCache(final long _c, final String _loc) {
        this.capacity = _c;
        this.location = _loc;
        map = new HashMap<String, CacheBlock>();
        delTPE = ThreadUtil.createPool("delete-pool", 2);
        writeTPE = ThreadUtil.createPool("write-pool", 50);

        LOG.info("LRUCache created with capacity {}, {}", this.capacity, this.location);
    }

    public S3ObjectInputStream get(final String key) {
        CacheBlock blk = null;

        synchronized (this) {
            blk = map.get(key);
            if (blk == null) {
                return null;
            }
        
            //Move to tail;
            removeBlock(blk);
            offerBlock(blk);
        }

        try {
            LOG.info("Found key {},{} in cache.", key, blk.size);
            final int readSize = 1024 * 1024;
            return new S3ObjectInputStream(new BufferedInputStream(new FileInputStream(new File(blk.location)),readSize), null);
        } catch (FileNotFoundException e) {
            LOG.error("Caught {}, {}", e, blk.location);
        }

        return null;
    }


    public void put(final String key, final byte[] data, final int size) {
            writeTPE.execute(new WriteFile(key, data, size, this));
    }

    public void writeFile(final String key, final byte[] data, final int size) {

        //If adding current size would go beyond capacity. Start deleting
        List<String> delList = new ArrayList<String>();
        synchronized (this) {
            while (currentSize + size > capacity) {
                LOG.info("Removing key {},{},{},{} from cache.", head.key, head.size, head.location, currentSize);
                map.remove(head.key);
                currentSize -= head.size;
                delList.add(head.location);
                removeBlock(head);
            }
        }

        if (delList.size() > 0) {
            delTPE.execute(new DeleteFiles(delList, this));
        }

        StringBuilder sb = new StringBuilder(location);
        sb.append(System.currentTimeMillis());
        sb.append('.').append(hashToBigInteger(key));
        String fileLoc = sb.toString();
        //add to tail
        CacheBlock blk = new CacheBlock(size, fileLoc, key);
    
        synchronized (this) {
            offerBlock(blk);
            currentSize += blk.size;
            map.put(key, blk);
            LOG.info("Adding key {},{},{}, to cache.", key, size, fileLoc);
        }

        //Create dir if necessary
        final int i = fileLoc.lastIndexOf("/");
        if (i > 0) {
            File dir = new File(fileLoc.substring(0, i));
            dir.mkdirs();
        }
        //write file
        if (flushToDisk(fileLoc, data, size) != true) {
            LOG.error("Write failed {}, {}", key, fileLoc);
            synchronized (this) {
                map.remove(key);
                currentSize -= blk.size;
                removeBlock(blk);
            }
        }
    }

    private void removeBlock(final CacheBlock n) {
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

    private void offerBlock(final CacheBlock n) {
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
    
    public void deleteFiles(List<String> keys) {
        for (final String key : keys) {
            deleteFile(key);
        }
    }

    private void deleteFile(final String key) {
        try {
            File f = new File(key);
            f.delete();
        } catch (Exception e) {
            LOG.warn("Caught exception {} when deleting {}", e, location+key);
        }
    }
    
    private boolean flushToDisk(final String fileLoc, final byte[] data, final int size) {
        File f = null;
        FileOutputStream fos = null;
        boolean pass = true;
        
        try {
            f = new File(fileLoc);
            fos = new FileOutputStream(f);
            fos.write(data, 0, size);
        } catch (Exception e) {
            pass = false;
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
            if (!pass && (f != null)) {
                deleteFile(fileLoc);
            }
        }
        return pass;
    }

    private BigInteger hashToBigInteger(final String token) {
        return hashToBigInteger(ByteBuffer.wrap(token.getBytes(UTF_8)));
    }
    private BigInteger hashToBigInteger(final ByteBuffer data) {
        byte[] result = hash(data);
        BigInteger hash = new BigInteger(result);
        return hash.abs();
    }

    private byte[] hash(final ByteBuffer... data) {

        MessageDigest messageDigest = CodecUtil.getMD5Digest();

        for (ByteBuffer block : data) {
            messageDigest.update(block.duplicate());
        }

        return messageDigest.digest();
    }
}

