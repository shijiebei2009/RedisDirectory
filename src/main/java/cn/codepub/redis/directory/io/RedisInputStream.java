package cn.codepub.redis.directory.io;

import cn.codepub.redis.directory.RedisFile;
import cn.codepub.redis.directory.utils.Constants;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * <p>
 * Created by wangxu on 16/10/27 18:11.
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
public class RedisInputStream extends IndexInput implements Cloneable {
    private RedisFile redisFile;
    private final long length;//the total length of the index file
    private byte[] currentBuffer;
    private int currentBufferIndex;
    private int bufferPosition;
    private int bufferLength;

    public RedisInputStream(String name, RedisFile redisFile) throws IOException {
        this(name, redisFile, redisFile.getFileLength());
    }

    private RedisInputStream(String name, RedisFile redisFile, long length) throws IOException {
        super("RedisInputStream(name=" + name + ")");
        this.redisFile = redisFile;
        this.length = length;
        if (this.length / Constants.BUFFER_SIZE >= Integer.MAX_VALUE) {
            throw new IOException("RedisInputStream too large length=" + length + ": " + name);
        }
        setCurrentBuffer();
    }

    /**
     * Get an unprocessed buffer with data to fill the currentBuffer
     */
    private void setCurrentBuffer() {
        if (currentBufferIndex < redisFile.numBuffers()) {
            currentBuffer = redisFile.getBuffer(currentBufferIndex);
            long bufferStart = (long) Constants.BUFFER_SIZE * (long) currentBufferIndex;
            bufferLength = (int) Math.min(Constants.BUFFER_SIZE, length - bufferStart);
        } else {
            currentBuffer = null;
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (bufferPosition == bufferLength) {
            nextBuffer();
        }
        return currentBuffer[bufferPosition++];
    }

    private void nextBuffer() throws IOException {
        if (getFilePointer() >= length) {
            throw new EOFException("cannot read another byte at EOF: pos=" + getFilePointer() + " vs length=" + length() + ": "
                    + this);
        }
        currentBufferIndex++;
        setCurrentBuffer();
        bufferPosition = 0;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (bufferPosition == bufferLength) {
                nextBuffer();
            }
            int remainInBuffer = bufferLength - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(currentBuffer, bufferPosition, b, offset, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
    }

    @Override
    public long getFilePointer() {
        return (long) currentBufferIndex * Constants.BUFFER_SIZE + bufferPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        int newBufferIndex = (int) (pos / Constants.BUFFER_SIZE);
        if (newBufferIndex != currentBufferIndex) {
            currentBufferIndex = newBufferIndex;
            setCurrentBuffer();
        }
        bufferPosition = (int) (pos % Constants.BUFFER_SIZE);
        if (getFilePointer() > length()) {
            throw new EOFException("seek beyond EOF: pos=" + getFilePointer() + " vs length=" + length() + ": " + this);
        }
    }

    @Override
    public void close() {
        // NOOP
        redisFile = null;
    }

    @Override
    public long length() {
        return this.length;
    }

    @Override
    public IndexInput slice(String sliceDescription, final long offset, final long sliceLength) throws IOException {
        if (offset < 0 || sliceLength < 0 || offset + sliceLength > this.length) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
        }
        return new RedisInputStream(getFullSliceDescription(sliceDescription), redisFile, offset + sliceLength) {
            {
                seek(0L);
            }

            @Override
            public void seek(long pos) throws IOException {
                if (pos < 0) {
                    throw new IllegalArgumentException("Seeking to negative position: " + this);
                }
                super.seek(pos + offset);
            }

            @Override
            public long getFilePointer() {
                return super.getFilePointer() - offset;
            }

            @Override
            public long length() {
                return sliceLength;
            }

            @Override
            public IndexInput slice(String sliceDescription, long ofs, long len) throws IOException {
                return super.slice(sliceDescription, offset + ofs, len);
            }
        };
    }
}
