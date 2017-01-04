package cn.codepub.redis.directory.io;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * <p>
 * Created by wangxu on 2016/12/26 15:08.
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
public interface InputOutputStream {
    Boolean hexists(final byte[] key, final byte[] field);

    byte[] hget(final byte[] key, final byte[] field);

    void close() throws IOException;

    Long hdel(final byte[] key, final byte[]... fields);

    Long hset(final byte[] key, final byte[] field, final byte[] value);

    Set<byte[]> hkeys(final byte[] key);

    default String[] getAllFileNames(String directoryMedata) {
        Objects.requireNonNull(directoryMedata);
        Set<byte[]> hkeys = hkeys(directoryMedata.getBytes());
        Objects.requireNonNull(hkeys);
        ArrayList<String> names = Lists.newArrayList();
        hkeys.forEach(key -> names.add(new String(key, StandardCharsets.UTF_8)));
        return names.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * Use transactions to delete index file
     *
     * @param fileLengthKey the key using for hash file length
     * @param fileDataKey   the key using for hash file data
     * @param field         the hash field
     * @param blockSize     the index file data block size
     */
    void deleteFile(String fileLengthKey, String fileDataKey, String field, long blockSize);

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
    void rename(String fileLengthKey, String fileDataKey, String oldField, String newField, List<byte[]> values, long
            fileLength);

    default void checkTransactionResult(List<Object> exec) {
        for (Object o : exec) {
            boolean equals = StringUtils.equals(Objects.toString(o), "0");
            if (equals) {
                System.err.println("Execute transaction occurs error!");
            }
        }
    }

    void saveFile(String fileLengthKey, String fileDataKey, String fileName, List<byte[]> values, long fileLength);

    List<byte[]> loadFileOnce(String fileDataKey, String fileName, long blockSize);
}
