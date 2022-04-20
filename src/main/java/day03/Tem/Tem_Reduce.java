package day03.Tem;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Tem_Reduce extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        //将map传输的数据进行处理\
        double sum ;
        DoubleWritable v3 = new DoubleWritable();
        Iterator<DoubleWritable> iterator = values.iterator();
        DoubleWritable next = iterator.next();
        sum = next.get();
        for (DoubleWritable value : values) {
            if (value.get()>sum) {
                sum=value.get();
            }
        }
        v3.set(sum);
        context.write(key,v3);
    }
}
