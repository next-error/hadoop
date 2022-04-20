package day03.Tem;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Tem_Mapper extends Mapper<LongWritable, Text,Text,DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        Text k2 = new Text();
        DoubleWritable v2 = new DoubleWritable();
        //切每一行数据.产生key和value
        String[] arr = value.toString().split(",");
        k2.set(arr[0]);
        v2.set(Double.parseDouble(arr[1]));
        context.write(k2,v2);
    }
}

