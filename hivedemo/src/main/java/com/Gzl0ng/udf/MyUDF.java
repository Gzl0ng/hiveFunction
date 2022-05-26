package com.Gzl0ng.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author 郭正龙
 * @date 2021-08-15
 */
public class MyUDF extends GenericUDF{
    //初始化,校验数据参数个数
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentException("参数个数不为1");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    //计算,处理数据的方法
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        //1.取出输入数据
        String input = deferredObjects[0].get().toString();

        //2.判断输入数据是否为null
        if (input == null){
            return 0;
        }
        //3.返回输入数据的长度
        return input.length();
    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}
