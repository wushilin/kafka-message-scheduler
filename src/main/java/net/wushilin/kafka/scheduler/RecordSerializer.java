package net.wushilin.kafka.scheduler;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RecordSerializer {
    public static byte[] serialize(Record<byte[], byte[]> record) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        // key
        // value
        // timestamp
        // header

        byte[] key = record.key();
        byte[] value = record.value();
        long timestamp = record.timestamp();

        Headers hdrs = record.headers();
        try {
            writeByteArray(bos, key);
            writeByteArray(bos, value);
            bos.write(toBytes(timestamp));
            bos.write(toBytes(hdrs));
            bos.flush();
            bos.close();
            return bos.toByteArray();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Record<byte[], byte[]> deserialize(byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte[] key = decodeBytes(bb);
        byte[] value = decodeBytes(bb);
        long timestamp = bb.getLong();
        Headers hdrs = decodeHeaders(bb);
        return new Record<byte[], byte[]>(key, value, timestamp, hdrs);
    }

    private static Headers decodeHeaders(ByteBuffer bb) {
        int size = bb.getInt();
        if(size == Integer.MAX_VALUE) {
            return null;
        }
        Header[] hs = new Header[size];
        for(int i = 0; i < hs.length; i++) {
            hs[i] = decodeHeader(bb);
        }
        Headers result = new RecordHeaders();
        for(Header next:hs) {
            result.add(next);
        }
        return result;
    }

    private static Header decodeHeader(ByteBuffer bb) {
        byte[] key = decodeBytes(bb);
        byte[] value = decodeBytes(bb);
        String keyS = null;
        if(key != null) {
            keyS = new String(key, StandardCharsets.UTF_8);
        }
        return new RecordHeader(keyS, value);
    }

    private static byte[] decodeBytes(ByteBuffer bb) {
        int size = bb.getInt();
        if(size == Integer.MAX_VALUE) {
            return null;
        }

        byte[] dst = new byte[size];
        bb.get(dst);
        return dst;
    }

    private static byte[] toBytes(Headers h) {
        Header[] hsr = h.toArray();
        if(hsr == null) {
            return toBytes(Integer.MAX_VALUE);
        }
        List<Header> hs = new ArrayList<Header>();
        for(Header next:hsr) {
            if(next != null) {
                hs.add(next);
            }
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(toBytes(hs.size()));
            for (Header next : hs) {
                byte[] key = next.key().getBytes(StandardCharsets.UTF_8);
                byte[] value = next.value();
                writeByteArray(bos, key);
                writeByteArray(bos, value);
            }
            return bos.toByteArray();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void writeNull(OutputStream os) throws IOException {
        os.write(toBytes(Integer.MAX_VALUE));
    }

    private static void writeByteArray(OutputStream os, byte[] toWrite) throws IOException {
        if(toWrite == null) {
            writeNull(os);
        } else {
            //write length first, then write content
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(toBytes(toWrite.length));
            bos.write(toWrite);
            os.write(bos.toByteArray());
        }
    }

    private static byte[] toBytes(int what) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(what);
        bb.flip();
        return bb.array();
    }

    private static byte[] toBytes(long what) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(what);
        bb.flip();
        return bb.array();
    }

    public static void main(String[] args) {
        for(int i = 0; i < 100000; i++) {
            Record<byte[], byte[]> test = new Record<>(randomBytes(), randomBytes(), System.currentTimeMillis(), null);
            byte[] data = serialize(test);
            Record<byte[], byte[]> decoded = deserialize(data);
            byte[] newdata = serialize(test);
            AssertEquals(test, decoded);
            AssertEquals(data, newdata);
        }
        System.out.println("Equal!");
    }

    public static Headers randomHeaders() {
        int max = 20;
        int length = rand.nextInt(max);
        if(length == max - 1) {
            return null;
        }

        Headers hdrs = new RecordHeaders();
        for(int i = 0; i < length; i++) {
            hdrs.add(new RecordHeader(randomKey(), randomBytes()));
        }
        return hdrs;
    }
    static Random rand = new Random();
    private static String randomKey() {
        int max = 20;
        int length = rand.nextInt(max);
        if(length == max - 1) {
            return null;
        }
        return RandomUtil.random(max);
    }

    public static byte[] randomBytes() {
        int max = 200;
        int length = rand.nextInt(max);
        if(length == max - 1) {
            return null;
        }
        byte[] result = new byte[length];
        rand.nextBytes(result);
        return result;
    }
    private static void AssertEquals(byte[] data1, byte[] data2) {
        if(arrayEQ(data1, data2)) {
            return;
        }
        throw new RuntimeException("Not equal");
    }
    private static void AssertEquals(Record<byte[], byte[]> first, Record<byte[], byte[]> second) {
        if(!arrayEQ(first.key(), second.key())) {
            throw new RuntimeException("Not equal");
        }
        if(!arrayEQ(first.value(), second.value())) {
            throw new RuntimeException("Not equal");
        }
        assertEQ(first.timestamp(), second.timestamp());
        assertEQ(first.headers(), second.headers());
    }

    private static void assertEQ(Headers first, Headers second) {
        if(first == second) {
            return;
        }
        if(first != null && second == null) {
            throw new RuntimeException("Not equal");
        }
        if(first == null && second != null) {
            throw new RuntimeException("Not equal");
        }
        if(first.toArray().length != second.toArray().length) {
            throw new RuntimeException("Not equal");
        }
        Header[] hs1 = first.toArray();
        Header[] hs2 = second.toArray();
        for(Header next:hs1) {
            if(!contains(second, next)) {
                throw new RuntimeException("Not equal");
            }
        }
        for(Header next:hs2) {
            if(!contains(first, next)) {
                throw new RuntimeException("Not equal");
            }
        }
    }

    private static boolean contains(Headers hs, Header what) {
        String key = what.key();
        byte[] value = what.value();

        Header[] all = hs.toArray();
        boolean found = false;
        for(Header next:all) {
            if(next.key().equals(key) && arrayEQ(value, next.value())) {
                found = true;
            }
        }
        return found;
    }
    private static void assertEQ(long first, long second) {
        if(first != second) {
            throw new RuntimeException("Not equal");
        }
    }

    private static boolean arrayEQ(byte[] first, byte[] second) {
        if(first == second) {
            return true;
        }

        if(first == null && second != null) {
            return false;
        }
        if(first != null && second == null) {
            return false;
        }

        if(first.length != second.length) {
            return false;
        }

        for(int i = 0; i < first.length; i++) {
            if(first[i] != second[i]) {
                return false;
            }
        }
        return true;
    }

}
