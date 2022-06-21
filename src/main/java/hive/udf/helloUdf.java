package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;


import org.junit.Test;

import java.text.ParseException;

/**
 * 功能描述  :UDF函数编写
 *
 * 什么是UDF： 用户定义函数(user-defined function)
 *
 * UDF干什么？ UDF操作作用于单个数据行，并且产生一个数据行作为输出。大多数函数都属于这一类（比如数学函数和字符串函数）。
 *
 *
 * 1、UDF函数自定义主要是继承hive-exec中UDF这个类，重写evaluate方法实现UDF自定义。
 * 2、临时函数的使用 ： 进入hive的交互shell中
         * 1. 创建存放jar的目录
         * [hdfs@dp-hadoop-3 jar]#   rz
         *
         * 2. 上传自定义udf的jar
         * hive> add jar /home/hadoop/jar/hive-udf-weekofyear-1.0-SNAPSHOT.jar
         *
         * 3. 创建临时函数
         * hive> create temporary function sayhelloUDF as 'org.hopson.helloUdf ';
         *
         * 4. 验证
         * hive> select sayhelloUDF ("Hello World!");
         *
 * 3、 永久函数的使用:
         * 1. 创建存放jar的目录
         *    hdfs dfs -mkdir -p /user/hiveUDF;
         *
         * 2. 把自定义函数的jar上传到hdfs中.
         *     hdfs dfs -put hive-udf-weekofyear-1.0-SNAPSHOT.jar  ' /user/hiveUDF/';
         *
         * 7. 创建永久函数
         * hive> create function sayhelloUDF as 'org.hopson.helloUdf' using jar 'hdfs:///path/user/hiveUDF/hive-udf-weekofyear-1.0-SNAPSHOT.jar';
         *
         * 8. 验证
         * hive> select sayhelloUDF  ("Hello World");
         * hive> show functions;
 *
 *
 * @author: dd
 * @date: 2022年06月21日 13:51
 */
public class helloUdf extends UDF {

    public String evaluate (String input){
        return "Hello:"+input;
    }

    //本地测试
    @Test
    public void Test() throws ParseException {
        String evaluate = evaluate("2018-12-31");
        System.out.println(evaluate);
    }



}
