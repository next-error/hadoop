package day03.countword;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Countreduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable v3 = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value : values) {
            sum+=value.get();
            v3.set(sum);
            context.write(key,v3);
        }
    }
}
