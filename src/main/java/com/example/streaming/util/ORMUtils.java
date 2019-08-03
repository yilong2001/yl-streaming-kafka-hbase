package com.example.streaming.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yilong on 2018/6/10.
 */
public class ORMUtils {
    volatile static String DEFAULT_CAT_CHAR = "\\.";

    private static Logger logger = Logger.getLogger(ORMUtils.class);

    public static Object getSqlFieldValue(Object element, String sqlField) {
        if (element instanceof GenericRecord) {
            return getAvroFieldValue(element, sqlField);
        }

        Object result = element;
        String[] sqlFields = sqlField.split(DEFAULT_CAT_CHAR);
        for (String fd : sqlFields) {
            if (isArrayField(fd)) {
                result = getArrayObject(result, fd);
            } else if (isMapField(fd)) {
                result = getMapObject(result, fd);
            } else {
                result = getSubElement(result, fd);
            }

            if (result == null) break;
        }

        return result;
    }

    private static Object getSubElement(Object element, String sqlField) {
        Class clazz = element.getClass();
        Field[] fields = clazz.getDeclaredFields();
        for (Field fd : fields) {
            if (fd.getName().equals(sqlField)) {
                logger.info(sqlField+" type : "+fd.getType().toGenericString());
                fd.setAccessible(true);
                try {
                    return fd.get(element);
                } catch (IllegalAccessException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

        return null;
    }

    private static boolean isArrayField(String sqlField) {
        String regx = "^[a-z|A-Z|_]\\w*\\[\\d+\\]";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        return m.find();
    }

    private static boolean isMapField(String sqlField) {
        String regx = "^[a-z|A-Z|_]\\w*\\[['|\"]\\w+['|\"]\\]";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        return m.find();
    }

    private static Object getArrayObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(\\d+)(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 4) {
            return null;
        }

        String field = m.group(1);
        int index = Integer.parseInt(m.group(3));
        Object fieldObj = getSubElement(element, field);
        if (fieldObj == null) {
            return null;
        }

        //for avro, array must be list
        if (fieldObj instanceof Collection) {
            Collection colobj = (Collection)fieldObj;
            if (colobj.size() <= index) {
                return null;
            }

            return colobj.toArray()[index];
        }

        //but, support array
        return Array.get(fieldObj, index);
    }

    private static Object getMapObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(['|\"])(\\w+)(['|\"])(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 6) {
            return null;
        }

        String field = m.group(1);
        String key = m.group(4);
        Object fieldObj = getSubElement(element, field);
        if (fieldObj == null) {
            return null;
        }

        if (!(fieldObj instanceof Map)) {
            return null;
        }

        Map map = (Map)fieldObj;
        return map.get(key.toString());
    }

    private static Object getAvroFieldValue(Object element, String sqlField) {
        Object result = element;

        String[] sqlFields = sqlField.split(DEFAULT_CAT_CHAR);
        for (String fd : sqlFields) {
            if (isArrayField(fd)) {
                result = getAvroArrayObject(result, fd);
            } else if (isMapField(fd)) {
                result = getAvroMapObject(result, fd);
            } else {
                result = getAvroSubElement(result, fd);
            }

            if (result == null) break;
        }

        return result;
    }

    private static Object getAvroSubElement(Object element, String field) {
        if (element instanceof GenericRecord) {
            return ((GenericRecord)element).get(field);
        }

        if (element instanceof Map) {
            return ((Map)element).get(field);
        }

        return null;
    }

    private static Object getAvroArrayObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(\\d+)(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 4) {
            return null;
        }

        String field = m.group(1);
        int index = Integer.parseInt(m.group(3));
        Object fieldObj = getAvroSubElement(element, field);
        if (fieldObj == null) {
            return null;
        }

        //for avro, array must be list
        return ((Collection)fieldObj).toArray()[index];
    }

    private static Object getAvroMapObject(Object element, String sqlField) {
        String regx = "^([a-z|A-Z|_]\\w*)(\\[)(['|\"])(\\w+)(['|\"])(\\])";
        Pattern r = Pattern.compile(regx);
        Matcher m = r.matcher(sqlField);
        if (!m.find() || m.groupCount() != 6) {
            return null;
        }

        String field = m.group(1);
        String key = m.group(4);
        Object fieldObj = getAvroSubElement(element, field);
        if (fieldObj == null) {
            return null;
        }

        if (!(fieldObj instanceof Map)) {
            return null;
        }

        Map map = (Map)fieldObj;
        return map.get(key.toString());
    }
}
