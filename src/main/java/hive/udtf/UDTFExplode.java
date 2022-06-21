package hive.udtf;


import java.util.ArrayList;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;



/**
 * 功能描述 ：自定义UDTF ： 使用UDTF对"Key：Value"这种字符串进行切分，返回结果为Key，Value两个字段。
 *
 * 什么是UDTF ：用户定义表生成函数（user-defined table-generating function ）
 *
 * UDTF干什么？ 操作作用于单个数据行，并且产生多个数据行-------一个表作为输出。lateral view explore()
 *
 * 有哪些函数是 UDTF？ 一个表作为输出，比如  lateral view explore()
 *
 *
 * 如何实现一个UDTF:
 *
 * UDTF(User-Defined Table-Generating Functions)用来解决输入一行输出多行(one-to-many maping)的需求。
 * 1.继承org.apache.hadoop.hive.ql.udf.generic.GenericUDTF。
 * 2.实现initialize()，process()，close()三个方法。
 * 3.UDTF首先会调用initialize()方法，此方法返回UDTF的返回行的信息(返回个数，类型)。
 * 4.初始化完成后会调用process()方法，对传入的参数进行处理，可以通过forward()方法把结果返回。
 * 5.最后调用close()对需要清理的方法进行清理。
 *
 *
 *
 *
 * @author: dd
 * @date: 2022年06月21日 14:17
 */
public class UDTFExplode extends GenericUDTF{

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }
    @Override
    public void process(Object[] args) throws HiveException {
        String input = args[0].toString();
        String[] test = input.split(";");
        for(int i=0; i<test.length; i++) {
            try {
                String[] result = test[i].split(":");
                forward(result);
            } catch (Exception e) {
                continue;
            }
        }
    }
}