package cn.codepub.redis.directory;

import cn.codepub.redis.directory.io.InputOutputStream;
import cn.codepub.redis.directory.io.RedisInputStream;
import cn.codepub.redis.directory.io.RedisOutputStream;
import cn.codepub.redis.directory.utils.Constants;
import com.google.common.primitives.Longs;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockName;
import static cn.codepub.redis.directory.utils.FileBlocksUtil.getBlockSize;


/**
 * <p>
 * Created by wangxu on 16/10/27 17:27.
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
public class RedisDirectory extends BaseDirectory implements Accountable {
    @Getter
    private InputOutputStream inputOutputStream;
    @Getter
    //key is index file name
    private static volatile Map<String, RedisFile> filesMap = new ConcurrentHashMap<>();
    @Getter
    private static final AtomicLong sizeInBytes = new AtomicLong();

    private RedisDirectory() throws IOException {
        super(new RedisLockFactory());
    }

    public RedisDirectory(InputOutputStream inputOutputStream) throws IOException {
        this();
        this.inputOutputStream = inputOutputStream;
    }

    /**
     * @return get all the file names lists
     */
    @Override
    public final String[] listAll() {
        ensureOpen();
        //directory->fileNames->fileLength，由fileLength%BLOCK_SIZE==0?fileLength/BLOCK_SIZE:fileLength/BLOCK_SIZE+1得到fileBlockSizes
        return inputOutputStream.getAllFileNames(Constants.dirMetadata);
    }

    /**
     * Returns true iff the named file exists in this directory.
     */
    private boolean fileNameExists(String fileName) {
        return inputOutputStream.hexists(Constants.dirMetadataBytes, fileName.getBytes());
    }

    /**
     * @param name file name
     * @return Returns the length of a file in the directory.
     * @throws IOException an I/O error
     */
    @Override
    public final long fileLength(String name) throws IOException {
        ensureOpen();
        long current = 0;
        byte[] b = inputOutputStream.hget(Constants.dirMetadataBytes, name.getBytes());
        if (b != null) {
            current = Longs.fromByteArray(b);
        }
        return current;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        boolean b = fileNameExists(name);
        if (b) {
            byte[] hget = inputOutputStream.hget(Constants.dirMetadataBytes, name.getBytes());
            long length = Longs.fromByteArray(hget);
            long blockSize = getBlockSize(length);
            inputOutputStream.deleteFile(Constants.dirMetadata, Constants.fileMetadata, name, blockSize);
        } else {
            log.error("Delete file {} does not exists!", name);
        }
    }

    /**
     * Creates a new, empty file in the directory with the given name. Returns a stream writing this file.
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return new RedisOutputStream(name, getInputOutputStream());
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        //NOOP, no operation
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        List<byte[]> values = new ArrayList<>();
        //在get的时候不需要加事务
        //在删除和添加的时候使用事务
        //Get the file length with old file name
        byte[] hget = inputOutputStream.hget(Constants.dirMetadataBytes, source.getBytes());
        long length = Longs.fromByteArray(hget);
        long blockSize = getBlockSize(length);
        for (int i = 0; i < blockSize; i++) {
            //Get the contents with old file name
            byte[] res = inputOutputStream.hget(Constants.fileMetadataBytes, getBlockName(source, i));
            values.add(res);
        }
        inputOutputStream.rename(Constants.dirMetadata, Constants.fileMetadata, source, dest, values, length);
        log.debug("Rename file success from {} to {}", source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        if (!fileNameExists(name)) {
            throw new FileNotFoundException(name);
        }
        //从redis中load文件到redis file对象中
        //单例Jedis有一个坑，就是在同一时刻只能被一个线程持有，在openInput方法中，Lucene会有Read操作和Merge操作，而其由不同的线程完成，所以如果在
        //loadRedisToFile中出现不同线程在瞬时同时持有Jedis对象会一直报错Socket Closed
        return new RedisInputStream(name, loadRedisToFile(name));
    }

    private RedisFile loadRedisToFile(String fileName) {
        byte[] hget = inputOutputStream.hget(Constants.dirMetadataBytes, fileName.getBytes());
        long lenght = Longs.fromByteArray(hget);
        RedisFile redisFile = new RedisFile(fileName, lenght);
        long blockSize = getBlockSize(lenght);
        List<byte[]> bytes = inputOutputStream.loadFileOnce(Constants.fileMetadata, fileName, blockSize);
        redisFile.setBuffers(bytes);
        return redisFile;
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        inputOutputStream.close();
    }

    /**
     * Return the memory usage of this object in bytes. Negative values are illegal.
     */
    @Override
    public long ramBytesUsed() {
        ensureOpen();
        return sizeInBytes.get();
    }


    /**
     * Returns nested resources of this class.
     * The result should be a point-in-time snapshot (to avoid race conditions).
     *
     * @see Accountables
     */
    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }
}
