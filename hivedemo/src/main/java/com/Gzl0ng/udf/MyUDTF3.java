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
 * 输入数据：hello,Gzl0ng:hello,hive
 * 输出数据:炸成二列
 * @author 郭正龙
 * @date 2021-08-15
 */
public class MyUDTF3 extends GenericUDTF{

    private ArrayList<String> outList = new ArrayList<String>();
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("word1");
        fieldNames.add("word2");

        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public void process(Object[] args) throws HiveException {

        //1.取出输入数据
        String input = args[0].toString();

        //2.按照“.”分隔字符串
        String[] fields = input.split(":");

        //3.遍历数据写出
        for (String field : fields) {

            //清空集合
            outList.clear();

            //将field按照‘，’分隔
            String[] words = field.split(",");

            //将words放入集合
            outList.add(words[0]);
            outList.add(words[1]);

            //写出数据
            forward(outList);
        }

    }

    public void close() throws HiveException {

    }
}
