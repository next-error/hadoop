package day03.countword;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Countmapper extends Mapper<LongWritable, Text, Text, IntWritable> {

Text k2 =new Text();
IntWritable v2 = new IntWritable();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split("\\s+");
        for (String s : arr) {
            k2.set(s);
            v2.set(1);
            context.write(k2,v2);
        }
    }


}
