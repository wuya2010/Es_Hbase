package common;

import java.io.File;
import java.io.IOException;

/**
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2017</p>
 * <p>Company: GZDX</p>
 *
 * @author TY
 * @version 1.0
 * @date 2017/12/28 18:15
 */
public class CreateWinutils {
    public static void createWinutils() {
        //windows解释hadoop路径
        File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        try {
            new File("./bin/winutils.exe").createNewFile();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
