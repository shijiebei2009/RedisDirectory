package cn.codepub.redis.directory.io;

import cn.codepub.redis.directory.utils.Constants;
import com.google.common.primitives.Longs;
import lombok.extern.log4j.Log4j2;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockName;
import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockSize;

/**
 * <p>
 * Created by wangxu on 2016/12/30 16:15.
 * </p>
 * <p>
 * Description: Using for Sharded Jedis
 * </p>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0 <br></br>
 * WebSite: http://codepub.cn <br></br>
 * Licence: Apache v2 License
 */
@Log4j2
public class ShardedJedisPoolStream implements InputOutputStream {
    private ShardedJedisPool shardedJedisPool;

    private ShardedJedis getShardedJedis() {
        return shardedJedisPool.getResource();
    }

    public ShardedJedisPoolStream(ShardedJedisPool shardedJedisPool) {
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        ShardedJedis shardedJedis = getShardedJedis();
        boolean hexists = shardedJedis.hexists(key, field);
        shardedJedis.close();
        return hexists;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        ShardedJedis shardedJedis = getShardedJedis();
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
        ShardedJedis shardedJedis = getShardedJedis();
        Long hdel = shardedJedis.hdel(key, fields);
        return hdel;
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        ShardedJedis shardedJedis = getShardedJedis();
        Long hset = shardedJedis.hset(key, field, value);
        shardedJedis.close();
        return hset;
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        ShardedJedis shardedJedis = getShardedJedis();
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
        ShardedJedis shardedJedis = getShardedJedis();
        ShardedJedisPipeline pipelined = shardedJedis.pipelined();
        //delete file length
        pipelined.hdel(fileLengthKey.getBytes(), field.getBytes());
        //delete file content
        for (int i = 0; i < blockSize; i++) {
            byte[] blockName = getBlockName(field, i);
            pipelined.hdel(fileDataKey.getBytes(), blockName);
        }
        pipelined.sync();
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
        ShardedJedis shardedJedis = getShardedJedis();
        ShardedJedisPipeline pipelined = shardedJedis.pipelined();
        //add new file length
        pipelined.hset(fileLengthKey.getBytes(), newField.getBytes(), Longs.toByteArray(fileLength));
        //add new file content
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            pipelined.hset(fileDataKey.getBytes(), getBlockName(newField, i), values.get(i));
        }
        pipelined.sync();
        shardedJedis.close();
        values.clear();
        deleteFile(fileLengthKey, fileDataKey, oldField, blockSize);
    }

    @Override
    public void saveFile(String fileLengthKey, String fileDataKey, String fileName, List<byte[]> values, long fileLength) {
        ShardedJedis shardedJedis = getShardedJedis();
        ShardedJedisPipeline pipelined = shardedJedis.pipelined();
        pipelined.hset(fileLengthKey.getBytes(), fileName.getBytes(), Longs.toByteArray(fileLength));
        Long blockSize = getBlockSize(fileLength);
        for (int i = 0; i < blockSize; i++) {
            pipelined.hset(fileDataKey.getBytes(), getBlockName(fileName, i), values.get(i));
        }
        pipelined.sync();
        shardedJedis.close();
        values.clear();
    }

    @Override
    public List<byte[]> loadFileOnce(String fileDataKey, String fileName, long blockSize) {
        ShardedJedis shardedJedis = getShardedJedis();
        ShardedJedisPipeline pipelined = shardedJedis.pipelined();
        List<byte[]> res = new ArrayList<>();
        List<Response<byte[]>> temps = new ArrayList<>();
        int temp = 0;
        //如果不分批次sync容易read time out和Java heap space
        while (temp < blockSize) {
            Response<byte[]> data = pipelined.hget(fileDataKey.getBytes(), getBlockName(fileName, temp));
            temps.add(data);
            if (temp % Constants.SYNC_COUNT == 0) {
                pipelined.sync();
                res.addAll(temps.stream().map(Response::get).collect(Collectors.toList()));
                temps.clear();
                pipelined = shardedJedis.pipelined();
            }
            temp++;
        }
        try {
            pipelined.sync();
        } catch (JedisConnectionException e) {
            log.error("pipelined = {}, blockSize = {}!", pipelined.toString(), blockSize);
            log.error("", e);
        } finally {
            shardedJedis.close();
        }
        res.addAll(temps.stream().map(Response::get).collect(Collectors.toList()));
        temps.clear();
        return res;
    }
}