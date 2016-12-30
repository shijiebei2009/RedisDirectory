package cn.codepub.redis.directory.utils;

import lombok.extern.log4j.Log4j2;

/**
 * <p>
 * Created by wangxu on 2016/12/26 15:40.
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
public class FileBlocksUtil {
    //public static String getBlockName(String fileName, int i) {
    //    return String.format("@%s:%s", fileName, i);
    //}

    public static byte[] getBlockName(String fileName, int i) {
        return String.format("@%s:%s", fileName, i).getBytes();
    }

    public static Long getBlockSize(long length) {
        return getBlockSize(length, Constants.BUFFER_SIZE);
    }

    private static Long getBlockSize(long length, long bufferSize) {
        if (bufferSize == 0) {
            log.error("Default buffer size is zero!");
            return 0L;
        }
        return length % bufferSize == 0 ? length / bufferSize : length / bufferSize + 1;
    }
}
