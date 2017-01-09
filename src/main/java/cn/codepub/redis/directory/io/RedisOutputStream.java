package cn.codepub.redis.directory.io;

import cn.codepub.redis.directory.RedisDirectory;
import cn.codepub.redis.directory.RedisFile;
import cn.codepub.redis.directory.util.Constants;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

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
public class RedisOutputStream extends IndexOutput {
    private InputOutputStream inputOutputStream;
    private RedisFile redisFile;
    private String indexFileName;
    private final Checksum crc;
    private byte[] currentBuffer;
    private int currentBufferIndex;
    private int bufferPosition;//记录当前buffer写到哪里了
    private long bufferStart;//记录当前buffer在总buffers的起始位置
    private int bufferLength;//记录当前buffer的长度

    public RedisOutputStream(String indexFileName, InputOutputStream inputOutputStream) {
        this(indexFileName, inputOutputStream, true);
    }

    private RedisOutputStream(String indexFileName, InputOutputStream inputOutputStream, boolean checksum) {
        super(indexFileName);
        this.currentBufferIndex = -1;
        this.currentBuffer = null;
        this.indexFileName = indexFileName;
        this.redisFile = new RedisFile();
        this.inputOutputStream = inputOutputStream;
        if (checksum) {
            crc = new BufferedChecksum(new CRC32());
        } else {
            crc = null;
        }
    }

    /**
     * first write to redis file, and now when close the stream, it will flush redis files to redis db
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        flushBuffers();
    }

    private void flushBuffers() {
        //先flush刷新索引文件长度
        setFileLength();
        List<byte[]> buffers = redisFile.getBuffers();
        inputOutputStream.saveFile(Constants.DIRECTORY_METADATA, Constants.FILE_METADATA, indexFileName, buffers, redisFile
                .getFileLength());
        RedisDirectory.getFilesMap().remove(indexFileName);
        RedisDirectory.getSizeInBytes().getAndAdd(-redisFile.ramBytesUsed());
        redisFile = null;
    }

    /**
     * update file length
     */
    private void setFileLength() {
        long pointer = bufferStart + bufferPosition;
        if (pointer > redisFile.getFileLength()) {
            redisFile.setLength(pointer);
        }
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (bufferPosition == bufferLength) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        if (crc != null) {
            crc.update(b);
        }
        currentBuffer[bufferPosition++] = b;
    }

    private void switchCurrentBuffer() {
        if (currentBufferIndex == redisFile.numBuffers()) {
            currentBuffer = redisFile.addBuffer(Constants.BUFFER_SIZE);
        } else {
            currentBuffer = redisFile.getBuffer(currentBufferIndex);
        }
        bufferPosition = 0;
        bufferStart = (long) Constants.BUFFER_SIZE * (long) currentBufferIndex;
        bufferLength = currentBuffer.length;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0) {
            return;
        }
        if (crc != null) {
            crc.update(b, offset, len);
        }
        while (len > 0) {
            if (bufferPosition == bufferLength) {
                currentBufferIndex++;
                switchCurrentBuffer();
            }
            int remainInBuffer = currentBuffer.length - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(b, offset, currentBuffer, bufferPosition, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
    }

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
    }

    @Override
    public long getChecksum() throws IOException {
        if (crc == null) {
            throw new IllegalStateException("internal RedisOutputStream created with checksum disabled");
        } else {
            return crc.getValue();
        }
    }

    @Override
    public String toString() {
        try {
            return indexFileName + "@" + getFilePointer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
