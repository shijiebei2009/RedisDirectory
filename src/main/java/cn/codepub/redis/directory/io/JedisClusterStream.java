package cn.codepub.redis.directory.io;

import cn.codepub.redis.directory.Operations;
import cn.codepub.redis.directory.util.CompressUtils;
import com.google.common.primitives.Longs;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static cn.codepub.redis.directory.util.CompressUtils.compressFilter;
import static cn.codepub.redis.directory.util.FileBlocksUtils.getBlockName;
import static cn.codepub.redis.directory.util.FileBlocksUtils.getBlockSize;

/**
 * <p>
 * Created by wangxu on 2016/12/26 15:18.
 * </p>
 * <p>
 * Description: Using for Jedis Cluster
 * </p>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0 <br></br>
 * WebSite: http://codepub.cn <br></br>
 * Licence: Apache v2 License
 */
public class JedisClusterStream implements InputOutputStream {
    private JedisCluster jedisCluster;

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return jedisCluster.hkeys(key);
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
        //delete file length
        jedisCluster.hdel(fileLengthKey.getBytes(), field.getBytes());
        //delete file content
        for (int i = 0; i < blockSize; i++) {
            byte[] blockName = getBlockName(field, i);
            jedisCluster.hdel(fileDataKey.getBytes(), blockName);
        }
    }

    @Override
    public void rename(String fileLengthKey, String fileDataKey, String oldField, String newField, List<byte[]> values, long
            fileLength) {
        //add new file length
        jedisCluster.hset(fileLengthKey.getBytes(), newField.getBytes(), Longs.toByteArray(fileLength));
        //add new file content
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            jedisCluster.hset(fileDataKey.getBytes(), getBlockName(newField, i), compressFilter(values.get(i)));
        }
        values.clear();
        deleteFile(fileLengthKey, fileDataKey, oldField, blockSize);
    }

    @Override
    public void saveFile(String fileLengthKey, String fileDataKey, String fileName, List<byte[]> values, long fileLength) {
        jedisCluster.hset(fileLengthKey.getBytes(), fileName.getBytes(), Longs.toByteArray(fileLength));
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            jedisCluster.hset(fileDataKey.getBytes(), getBlockName(fileName, i), compressFilter(values.get(i)));
        }
        values.clear();
    }

    @Override
    public List<byte[]> loadFileOnce(String fileDataKey, String fileName, long blockSize) {
        List<byte[]> res = new ArrayList<>();
        for (int i = 0; i < blockSize; i++) {
            byte[] hget = jedisCluster.hget(fileDataKey.getBytes(), getBlockName(fileName, i));
            res.add(hget);
        }
        return res;
    }

    public JedisClusterStream(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return jedisCluster.hexists(key, field);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field, Operations operations) {
        byte[] hget = jedisCluster.hget(key, field);
        if (operations == Operations.FILE_DATA) {
            return CompressUtils.uncompressFilter(hget);
        }
        return hget;
    }

    @Override
    public void close() throws IOException {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        return jedisCluster.hdel(key, fields);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value, Operations operations) {
        if (operations == Operations.FILE_DATA) {
            value = compressFilter(value);
        }
        return jedisCluster.hset(key, field, value);
    }
}
