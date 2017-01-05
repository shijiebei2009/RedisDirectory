package cn.codepub.redis.directory.io;

import cn.codepub.redis.directory.utils.Constants;
import com.google.common.primitives.Longs;
import lombok.extern.log4j.Log4j2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockName;
import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockSize;

/**
 * <p>
 * Created by wangxu on 2016/12/26 15:12.
 * </p>
 * <p>
 * Description: Using for Jedis Pool
 * </p>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0 <br></br>
 * WebSite: http://codepub.cn <br></br>
 * Licence: Apache v2 License
 */
@Log4j2
public class JedisPoolStream implements InputOutputStream {
    private JedisPool jedisPool;

    public JedisPoolStream(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        boolean hexists;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            hexists = jedis.hexists(key, field);
        } finally {
            jedis.close();
        }
        return hexists;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        byte[] hget;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            hget = jedis.hget(key, field);
        } finally {
            jedis.close();
        }
        return hget;
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        long hdel;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            hdel = jedis.hdel(key, fields);
        } finally {
            jedis.close();
        }
        return hdel;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        Long hset;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            hset = jedis.hset(key, field, value);
        } finally {
            jedis.close();
        }
        return hset;
    }


    @Override
    public Set<byte[]> hkeys(byte[] key) {
        Set<byte[]> hkeys;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            hkeys = jedis.hkeys(key);
        } finally {
            jedis.close();
        }
        return hkeys;
    }

    /**
     * Use transactions to delete index file
     *
     * @param fileLengthKey
     * @param fileDataKey
     * @param field
     * @param blockSize
     */
    @Override
    public void deleteFile(String fileLengthKey, String fileDataKey, String field, long blockSize) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Pipeline pipelined = jedis.pipelined();
            //delete file length
            pipelined.hdel(fileLengthKey.getBytes(), field.getBytes());
            //delete file content
            for (int i = 0; i < blockSize; i++) {
                byte[] blockName = getBlockName(field, i);
                pipelined.hdel(fileDataKey.getBytes(), blockName);
            }
            pipelined.sync();
        } finally {
            jedis.close();
        }
    }

    @Override
    public void rename(String fileLengthKey, String fileDataKey, String oldField, String newField, List<byte[]> values, long
            fileLength) {
        long blockSize = 0;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Pipeline pipelined = jedis.pipelined();
            //add new file length
            pipelined.hset(fileLengthKey.getBytes(), newField.getBytes(), Longs.toByteArray(fileLength));
            //add new file content
            blockSize = getBlockSize(fileLength);
            for (int i = 0; i < blockSize; i++) {
                pipelined.hset(fileDataKey.getBytes(), getBlockName(newField, i), values.get(i));
            }
            values.clear();
            pipelined.sync();
        } finally {
            jedis.close();
            deleteFile(fileLengthKey, fileDataKey, oldField, blockSize);
        }
    }

    @Override
    public void saveFile(String fileLengthKey, String fileDataKey, String fileName, List<byte[]> values, long fileLength) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            Pipeline pipelined = jedis.pipelined();
            pipelined.hset(fileLengthKey.getBytes(), fileName.getBytes(), Longs.toByteArray(fileLength));
            Long blockSize = getBlockSize(fileLength);
            for (int i = 0; i < blockSize; i++) {
                pipelined.hset(fileDataKey.getBytes(), getBlockName(fileName, i), values.get(i));
            }
            values.clear();
            pipelined.sync();
        } finally {
            jedis.close();
        }
    }

    @Override
    public List<byte[]> loadFileOnce(String fileDataKey, String fileName, long blockSize) {
        Jedis jedis = jedisPool.getResource();
        Pipeline pipelined = jedis.pipelined();
        List<byte[]> res = new ArrayList<>();
        List<Response<byte[]>> temps = new ArrayList<>();
        int temp = 0;
        while (temp < blockSize) {
            Response<byte[]> data = pipelined.hget(fileDataKey.getBytes(), getBlockName(fileName, temp));
            temps.add(data);
            if (temp % Constants.SYNC_COUNT == 0) {
                pipelined.sync();
                res.addAll(temps.stream().map(Response::get).collect(Collectors.toList()));
                pipelined = jedis.pipelined();
                temps.clear();
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
        res.addAll(temps.stream().map(Response::get).collect(Collectors.toList()));
        temps.clear();
        return res;
    }
}
