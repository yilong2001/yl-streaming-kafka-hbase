package com.example.streaming.myudf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yilong on 2019/4/18.
 */
public class IsAbnormal {
    static public Boolean isAbnormalEx(Integer grade, String name) throws Exception {
        if (grade == null || name == null) {
            return false;
        }
        if (grade >= 60 && name.length() > 0)
            return true;
        return false;
    }
}
