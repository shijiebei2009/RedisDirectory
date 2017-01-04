package cn.codepub.redis.directory;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.lucene.util.Accountable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * Created by wangxu on 2016/11/09 10:43.
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
@NoArgsConstructor
@ToString
public class RedisFile implements Accountable {
    @Getter
    @Setter
    private List<byte[]> buffers = new ArrayList<>();
    @Getter
    @Setter
    private String fileName;
    @Getter
    private byte[] filePath;
    private long fileLength;
    private volatile boolean isDirty;
    protected long sizeInBytes;

    public RedisFile(String fileName, long fileLength) {
        this.fileName = fileName;
        this.fileLength = fileLength;
    }

    /**
     * Return the memory usage of this object in bytes. Negative values are illegal.
     */
    @Override
    public long ramBytesUsed() {
        return sizeInBytes;
    }

    /**
     * Returns nested resources of this class.
     * The result should be a point-in-time snapshot (to avoid race conditions).
     *
     * @see org.apache.lucene.util.Accountables
     */
    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    public final byte[] addBuffer(int size) {
        byte[] buffer = newBuffer(size);
        synchronized (this) {
            buffers.add(buffer);
            sizeInBytes += size;
        }
        RedisDirectory.getSizeInBytes().getAndAdd(size);
        return buffer;
    }

    private byte[] newBuffer(int size) {
        return new byte[size];
    }

    public final synchronized byte[] getBuffer(int index) {
        return buffers.get(index);
    }

    public final synchronized int numBuffers() {
        return buffers.size();
    }

    public synchronized long getFileLength() {
        return fileLength;
    }

    public synchronized void setLength(long length) {
        this.fileLength = length;
    }
}