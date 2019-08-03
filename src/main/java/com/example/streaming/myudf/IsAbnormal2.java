package com.example.streaming.myudf;

import java.io.Serializable;

/**
 * Created by yilong on 2019/4/18.
 */
public class IsAbnormal2 implements Serializable {
    public Boolean isAbnormalEx(Integer grade, String name) throws Exception {
        if (grade == null || name == null) {
            return false;
        }
        if (grade >= 60 && name.length() > 0)
            return true;
        return false;
    }
}
