package cn.codepub.redis.directory.util;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * <p>
 * Created by wangxu on 2016/12/09 15:18.
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
public interface Constants {
    int BUFFER_SIZE = NumberUtils.toInt(ConfigUtils.getValue("DEFAULT_BUFFER_SIZE"));
    //directoryMetadata=>list(index file name)=>list(index file length)
    String DIRECTORY_METADATA = ConfigUtils.getValue("DEFAULT_DIRECTORY_METADATA");
    //fileMetada=>list(@index file name:block number)=>list(the value of index file block)
    String FILE_METADATA = ConfigUtils.getValue("DEFAULT_FILE_DATA");
    int BUFFER_SIZE_IN_MEM = NumberUtils.toInt(ConfigUtils.getValue("DEFAULT_BUFFER_SIZE_IN_MEM"));
    String LOCK_FILE_PATH = ConfigUtils.getValue("LOCK_FILE_PATH");
    byte[] DIR_METADATA_BYTES = DIRECTORY_METADATA.getBytes();
    byte[] FILE_METADATA_BYTES = FILE_METADATA.getBytes();
    int TIME_OUT = NumberUtils.toInt(ConfigUtils.getValue("TIME_OUT"));
    int SYNC_COUNT = 50;//每隔count调用一次sync
    boolean COMPRESS_FILE = BooleanUtils.toBoolean(ConfigUtils.getValue("COMPRESS_FILE"));
}
