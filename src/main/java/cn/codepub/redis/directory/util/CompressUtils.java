package cn.codepub.redis.directory.util;

import lombok.extern.log4j.Log4j2;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 * Created by wangxu on 2017/01/09 9:52.
 * </p>
 * <p>
 * Description: Compress File data if needed
 * </p>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0 <br></br>
 * WebSite: http://codepub.cn <br></br>
 * Licence: Apache v2 License
 */
@Log4j2
public class CompressUtils {
    public static byte[] compressFilter(byte[] datas) {
        if (Constants.COMPRESS_FILE) {
            try {
                datas = Snappy.compress(datas);
            } catch (IOException e) {
                log.error("Compress error!", e);
            }
        }
        return datas;
    }

    public static byte[] uncompressFilter(byte[] datas) {
        if (Constants.COMPRESS_FILE) {
            try {
                datas = Snappy.uncompress(datas);
            } catch (IOException e) {
                log.error("Uncompress error!", e);
            }
        }
        return datas;
    }

    public static List<byte[]> uncompressFilter(List<byte[]> values) {
        List<byte[]> res = values.stream().map(CompressUtils::uncompressFilter).collect(Collectors.toList());
        return res;
    }
}
