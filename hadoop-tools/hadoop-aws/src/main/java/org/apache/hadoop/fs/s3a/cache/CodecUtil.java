package org.apache.hadoop.fs.s3a.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class CodecUtil {

    private static final MessageDigestThreadLocal MD5 = new MessageDigestThreadLocal("MD5");

    public static MessageDigest getMD5Digest() {
        return MD5.get();
    }

    private static class MessageDigestThreadLocal extends ThreadLocal<MessageDigest> {

        private String algorithm;

        protected MessageDigestThreadLocal(final String algorithm) {
            this.algorithm = algorithm;
        }

        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance(algorithm);
            } catch (NoSuchAlgorithmException ex) {
                throw new ExceptionInInitializerError(ex);
            }
        }

        @Override
        public MessageDigest get() {
            MessageDigest md = super.get();
            md.reset();
            return md;
        }
    };
}

