package cn.codepub.redis.directory.io;

import com.google.common.primitives.Longs;
import lombok.extern.log4j.Log4j2;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Set;

import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockName;
import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockSize;

/**
 * <p>
 * Created by wangxu on 2016/12/26 15:12.
 * </p>
 * <p>
 * Description: TODO
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
        Jedis jedis = jedisPool.getResource();
        Boolean hexists = jedis.hexists(key, field);
        jedis.close();
        return hexists;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        Jedis jedis = jedisPool.getResource();
        byte[] hget = jedis.hget(key, field);
        jedis.close();
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
        Jedis jedis = jedisPool.getResource();
        Long hdel = jedis.hdel(key, fields);
        jedis.close();
        return hdel;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = jedisPool.getResource();
        Long hset = jedis.hset(key, field, value);
        jedis.close();
        return hset;
    }


    @Override
    public Set<byte[]> hkeys(byte[] key) {
        Jedis jedis = jedisPool.getResource();
        Set<byte[]> hkeys = jedis.hkeys(key);
        jedis.close();
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
        Jedis jedis = jedisPool.getResource();
        Transaction multi = jedis.multi();
        //delete file length
        multi.hdel(fileLengthKey.getBytes(), field.getBytes());
        //delete file content
        for (int i = 0; i < blockSize; i++) {
            byte[] blockName = getBlockName(field, i);
            multi.hdel(fileDataKey.getBytes(), blockName);
        }
        List<Object> exec = multi.exec();
        checkTransactionResult(exec);
        multi.clear();
        jedis.close();
    }

    @Override
    public void rename(String fileLengthKey, String fileDataKey, String oldField, String newField, List<byte[]> values, long
            fileLength) {
        Jedis jedis = jedisPool.getResource();
        Transaction multi = jedis.multi();
        //add new file length
        multi.hset(fileLengthKey.getBytes(), newField.getBytes(), Longs.toByteArray(fileLength));
        //add new file content
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            multi.hset(fileDataKey.getBytes(), getBlockName(newField, i), values.get(i));
        }
        values.clear();
        List<Object> exec = multi.exec();
        checkTransactionResult(exec);
        multi.clear();
        jedis.close();
        deleteFile(fileLengthKey, fileDataKey, oldField, blockSize);
    }


}
