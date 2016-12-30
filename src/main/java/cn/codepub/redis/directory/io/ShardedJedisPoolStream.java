package cn.codepub.redis.directory.io;

import com.google.common.primitives.Longs;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockName;
import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockSize;

/**
 * <p>
 * Created by wangxu on 2016/12/30 16:15.
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
public class ShardedJedisPoolStream implements InputOutputStream {
    private ShardedJedisPool shardedJedisPool;

    public ShardedJedisPoolStream(ShardedJedisPool shardedJedisPool) {
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        Boolean hexists = shardedJedis.hexists(key, field);
        shardedJedis.close();
        return hexists;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        byte[] hget = shardedJedis.hget(key, field);
        shardedJedis.close();
        return hget;
    }

    @Override
    public void close() throws IOException {
        if (shardedJedisPool != null) {
            shardedJedisPool.close();
        }
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        Long hdel = shardedJedis.hdel(key, fields);
        return hdel;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        Long hset = shardedJedis.hset(key, field, value);
        shardedJedis.close();
        return hset;
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        Set<byte[]> hkeys = shardedJedis.hkeys(key);
        shardedJedis.close();
        return hkeys;
    }

    /**
     * Use transactions to delete index file
     *
     * @param fileLengthKey the key using for hash file length
     * @param fileDataKey   the key using for hash file data
     * @param field         the hash field
     * @param blockSize     the index file data block size
     */
    @Override
    public void deleteFile(String fileLengthKey, String fileDataKey, String field, long blockSize) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        //delete file length
        shardedJedis.hdel(fileLengthKey.getBytes(), field.getBytes());
        //delete file content
        for (int i = 0; i < blockSize; i++) {
            byte[] blockName = getBlockName(field, i);
            shardedJedis.hdel(fileDataKey.getBytes(), blockName);
        }
        shardedJedis.close();
    }

    /**
     * Use transactions to add index file and then delete the old one
     *
     * @param fileLengthKey the key using for hash file length
     * @param fileDataKey   the key using for hash file data
     * @param oldField      the old hash field
     * @param newField      the new hash field
     * @param values        the data values of the old hash field
     * @param fileLength    the data length of the old hash field
     */
    @Override
    public void rename(String fileLengthKey, String fileDataKey, String oldField, String newField, List<byte[]> values, long
            fileLength) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        //add new file length
        shardedJedis.hset(fileLengthKey.getBytes(), newField.getBytes(), Longs.toByteArray(fileLength));
        //add new file content
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            shardedJedis.hset(fileDataKey.getBytes(), getBlockName(newField, i), values.get(i));
        }
        values.clear();
        shardedJedis.close();
        deleteFile(fileLengthKey, fileDataKey, oldField, blockSize);
    }
}
