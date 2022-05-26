package com.Gzl0ng.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 输入数据:hello,Gzl0ng,hive
 * 输出数据:
 *      hello
 *      Gzl0ng
 *      hive
 * @author 郭正龙
 * @date 2021-08-15
 */
public class MyUDTF extends GenericUDTF {

    //输出数据的集合
    private ArrayList<String> outputList = new ArrayList<String>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //输出数据的默认列名，可以别名覆盖
        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("words");

        //输出数据的类型
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //最终返回值
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //处理输入数据:hello,Gzl0ng,hive
    public void process(Object[] objects) throws HiveException {
        //1.取出输入数据
        String input = objects[0].toString();

        //2.按照逗号分隔
        String[] words = input.split(",");

        //3.遍历数据写出
        for (String word:words){
            //清空集合
            outputList.clear();

            //将数据放入集合
            outputList.add(word);

            //输出数据
            forward(outputList);
        }
    }

    //收尾
    public void close() throws HiveException {

    }
}
