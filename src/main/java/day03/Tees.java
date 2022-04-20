package day03;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
/*
MR过程:
1.split切片开始--->InputFormat(抽象)TextInputFormat的getSplits获取切片;createRecordReader方法创造流读取切片内容,返回LineRecordReader方法一行一行读取
2.读到的内容用自己写的Mapper业务逻辑循环处理,输出K2,V2
3.K2,V2写入文件(需要与操作系统IO交互),采用KVBuffer的方式读写数据;溢写时会进行分区(Partitioner抽象(HashPartitioner的getPartition方法
    ) 和 排序 (key的compareTo方法,快速排序)
4.有一个split产生的多个已经分区排序的小文件merge归并为一个大文件
5.Reducer经过网络传输读取到多个split产生的文件,再2merge归并为一个大文件
6.WritableComparator排为(key,{1,1,1,...})的形式交给自己写的Reducer逻辑处理
7.处理完毕后OutputFormat(抽象)的TextOutputFormat输出????也是kvbuffer吗??????



 */
public class Tees {
    public static void main(String[] args) throws FileNotFoundException {
        //获取切片的方法---in.getSplits();
        //读取切片数据---createRecordReader
        InputFormat in = new FileInputFormat() {
            @Override
            public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                return null;
            }
        };



    }
}
