package cn.codepub.redis.directory.utils;

import com.google.common.base.Strings;
import lombok.extern.log4j.Log4j2;

import java.util.ResourceBundle;

/**
 * <p>
 * Created with IntelliJ IDEA. 16/10/28 18:00
 * </p>
 * <p>
 * Description: Get some default value from config file
 * </P>
 *
 * @author Wang Xu
 * @version V1.0.0
 * @since V1.0.0
 */
@Log4j2
public class ConfigUtils {
    private ConfigUtils() {
    }

    //default config file name
    private static ResourceBundle resourceBundle = ResourceBundle.getBundle("config");

    public static void setResourceBundle(String fileName) {
        resourceBundle = ResourceBundle.getBundle(fileName);
    }

    /**
     * @param key key in config file
     * @return value
     */
    public static String getValue(String key) {
        boolean nullOrEmpty = Strings.isNullOrEmpty(key);
        String res = null;
        if (!nullOrEmpty && resourceBundle != null) {
            res = resourceBundle.getString(key);
            log.debug("key = " + key + ", value = " + res);
        } else {
            log.error("Key or resource bundle is null or empty!");
        }
        return res;
    }
}

