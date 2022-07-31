package org.apache.hadoop.fs.s3a.cache;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.ByteArrayInputStream;
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


public class LRUCache2 {

    private class WriteFile implements Runnable {
        private String key;
        private byte[] data;
        private int size;
        private LRUCache2 cache;

        WriteFile(final String _key, final byte[] _data, final int _size, final LRUCache2 _cache) {
            this.key = _key;
            this.data = _data;
            this.size = _size;
            this.cache = _cache;
        }
        
        public void run() {
            cache.writeFile(key, data, size);
        }
    }


    long capacity = 0L;
    long currentSize = 0L;
    String location;
    HashMap<String, byte[]> map = null;
    ThreadPoolExecutor writeTPE;

    
    private static final Logger LOG =
        LoggerFactory.getLogger(LRUCache2.class);
	

    public LRUCache2(final long _c, final String _loc) {
        this.capacity = _c;
        this.location = _loc;
        map = new HashMap<String, byte[]>();
        writeTPE = ThreadUtil.createPool("write-pool", 50);

        LOG.info("LRUCache2 created with capacity {}, {}", this.capacity, this.location);
    }

    public S3ObjectInputStream get(final String key) {

        synchronized (this) {
            byte[] data = map.get(key);
            if (data != null) {
                //found in memory cache
                return new S3ObjectInputStream(new ByteArrayInputStream(data), null);
            }
        }

        CacheClient client = new CacheClient();
        String location[] = client.getKey(key);
        if (location != null && location.length == 2) {
            try {
                int readSize = 2 * 1024 * 1024;
                int cl = 0;
                try {
                    cl = Integer.parseInt(location[1]);
                } catch (Exception e ) {
                    LOG.error("Caught exception {}", e);
                }

                if (cl > 0 && cl <= capacity) {
                    byte[] data = new byte[cl];
                    FileInputStream fis = null;
                    try {
                        fis = new FileInputStream(new File(location[0]));
                        int read = 0;
                        int off = 0;
                        int toRead = 0;
                        while (off < data.length && read >=0) {
                            toRead = data.length - off;
                            read = fis.read(data, off, toRead);
                            off += read;
                        }
                        synchronized (this) {
                            map.put(key, data);
                        }
                        return new S3ObjectInputStream(new ByteArrayInputStream(data), null);
                    } catch (Exception e) {
                        LOG.error("Caught exception {}", e);
                        return null;
                    } finally {
                        if (fis != null) {
                            try {
                                fis.close();
                            } catch (Exception e) {
                                LOG.error("Caught exception {}", e);
                            }
                        }
                    }
                }

                return new S3ObjectInputStream(new BufferedInputStream(new FileInputStream(new File(location[0])), readSize), null);
            } catch (FileNotFoundException e) {
                LOG.error("Caught {}, {}", e, location);
            }
        }

        return null;
    }


    public void put(final String key, final byte[] data, final int size) {
            writeTPE.execute(new WriteFile(key, data, size, this));
    }

    public void writeFile(final String key, final byte[] data, final int size) {

        if (size <= capacity) {
            //Anything less than 1M. Keep in memory
            synchronized(this) {
                map.put(key, data);
            }
        }

        StringBuilder sb = new StringBuilder(location);
        sb.append(System.currentTimeMillis());
        sb.append('.').append(hashToBigInteger(key));
        String fileLoc = sb.toString();
    
        //Create dir if necessary
        final int i = fileLoc.lastIndexOf("/");
        if (i > 0) {
            File dir = new File(fileLoc.substring(0, i));
            dir.mkdirs();
        }
        //write file
        if (flushToDisk(fileLoc, data, size) != true) {
            LOG.error("Write failed {}, {}", key, fileLoc);
            return;
        }
        CacheClient client = new CacheClient();
        client.putKey(key, fileLoc, size);
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

    private void deleteFile(final String key) {
        try {
            File f = new File(key);
            f.delete();
        } catch (Exception e) {
            LOG.warn("Caught exception {} when deleting {}", e, location+key);
        }
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

