package cn.codepub.redis.directory.io;

import cn.codepub.redis.directory.Operations;
import cn.codepub.redis.directory.util.Constants;
import com.google.common.primitives.Longs;
import lombok.extern.log4j.Log4j2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cn.codepub.redis.directory.util.CompressUtils.compressFilter;
import static cn.codepub.redis.directory.util.CompressUtils.uncompressFilter;
import static cn.codepub.redis.directory.util.FileBlocksUtils.getBlockName;
import static cn.codepub.redis.directory.util.FileBlocksUtils.getBlockSize;


/**
 * <p>
 * Created by wangxu on 2016/12/26 16:18.
 * </p>
 * <p>
 * Description: Using for Jedis
 * </p>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0 <br></br>
 * WebSite: http://codepub.cn <br></br>
 * Licence: Apache v2 License
 */
@Log4j2
public class JedisStream implements InputOutputStream {
    private String IP;
    private int port;
    private int timeout = Constants.TIME_OUT;

    private Jedis openJedis() {
        return new Jedis(IP, port, this.timeout);
    }

    public JedisStream(String IP, int port, int timeout) {
        this(IP, port);
        this.timeout = timeout;
    }

    public JedisStream(String IP, int port) {
        this.IP = IP;
        this.port = port;
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        boolean hexists;
        Jedis jedis = null;
        try {
            jedis = openJedis();
            hexists = jedis.hexists(key, field);
        } finally {
            jedis.close();
        }
        return hexists;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field, Operations operations) {
        Jedis jedis = null;
        byte[] hget;
        try {
            jedis = openJedis();
            hget = jedis.hget(key, field);
            if (operations == Operations.FILE_DATA) {
                return uncompressFilter(hget);
            }
        } finally {
            jedis.close();
        }
        return hget;
    }

    @Override
    public void close() throws IOException {
        //Noop
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        Jedis jedis = openJedis();
        Long hdel = jedis.hdel(key, fields);
        jedis.close();
        return hdel;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value, Operations operations) {
        Jedis jedis = openJedis();
        if (operations == Operations.FILE_DATA) {
            value = compressFilter(value);
        }
        Long hset = jedis.hset(key, field, value);
        jedis.close();
        return hset;
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        Jedis jedis = openJedis();
        Set<byte[]> hkeys = jedis.hkeys(key);
        jedis.close();
        return hkeys;
    }

    @Override
    public void deleteFile(String fileLengthKey, String fileDataKey, String field, long blockSize) {
        Jedis jedis = openJedis();
        Pipeline pipelined = jedis.pipelined();
        //delete file length
        pipelined.hdel(fileLengthKey.getBytes(), field.getBytes());
        //delete file content
        for (int i = 0; i < blockSize; i++) {
            byte[] blockName = getBlockName(field, i);
            pipelined.hdel(fileDataKey.getBytes(), blockName);
        }
        pipelined.sync();
        jedis.close();
    }

    @Override
    public void rename(String fileLengthKey, String fileDataKey, String oldField, String newField, List<byte[]> values, long
            fileLength) {
        Jedis jedis = openJedis();
        Pipeline pipelined = jedis.pipelined();
        //add new file length
        pipelined.hset(fileLengthKey.getBytes(), newField.getBytes(), Longs.toByteArray(fileLength));
        //add new file content
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            pipelined.hset(fileDataKey.getBytes(), getBlockName(newField, i), compressFilter(values.get(i)));
        }
        pipelined.sync();
        jedis.close();
        values.clear();
        deleteFile(fileLengthKey, fileDataKey, oldField, blockSize);
    }

    @Override
    public void saveFile(String fileLengthKey, String fileDataKey, String fileName, List<byte[]> values, long fileLength) {
        Jedis jedis = openJedis();
        Pipeline pipelined = jedis.pipelined();
        pipelined.hset(fileLengthKey.getBytes(), fileName.getBytes(), Longs.toByteArray(fileLength));
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            pipelined.hset(fileDataKey.getBytes(), getBlockName(fileName, i), compressFilter(values.get(i)));
            if (i % Constants.SYNC_COUNT == 0) {
                pipelined.sync();
                pipelined = jedis.pipelined();
            }
        }
        pipelined.sync();
        jedis.close();
        values.clear();
    }

    @Override
    public List<byte[]> loadFileOnce(String fileDataKey, String fileName, long blockSize) {
        Jedis jedis = openJedis();
        Pipeline pipelined = jedis.pipelined();
        List<byte[]> res = new ArrayList<>();
        List<Response<byte[]>> temps = new ArrayList<>();
        int temp = 0;
        while (temp < blockSize) {
            Response<byte[]> data = pipelined.hget(fileDataKey.getBytes(), getBlockName(fileName, temp));
            temps.add(data);
            if (temp % Constants.SYNC_COUNT == 0) {
                pipelined.sync();
                res.addAll(temps.stream().map(response -> uncompressFilter(response.get())).collect(Collectors.toList()));
                temps.clear();
                pipelined = jedis.pipelined();
            }
            temp++;
        }
        try {
            pipelined.sync();
        } catch (JedisConnectionException e) {
            log.error("pipelined = {}, blockSize = {}!", pipelined.toString(), blockSize);
            log.error("", e);
        } finally {
            jedis.close();
        }
        res.addAll(temps.stream().map(response -> uncompressFilter(response.get())).collect(Collectors.toList()));
        temps.clear();
        return res;
    }
}
