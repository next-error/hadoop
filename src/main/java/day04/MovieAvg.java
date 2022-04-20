package day04;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MovieAvg {
    private static class MovieMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            Text k2 = new Text();
            DoubleWritable v2 = new DoubleWritable();
            String line = value.toString();
            Movie movie = JSON.parseObject(line, Movie.class);
            k2.set(movie.getMovie());
            v2.set(movie.getRate());
            context.write(k2,v2);
        }
    }
    private static class MovieReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        double sum =0;
        int count =0;
        DoubleWritable v3 = new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                sum+=value.get();
                count++;
            }
            v3.set(sum/count);
            context.write(key,v3);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof, "movie");
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job,new Path("d:\\g\\movie.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:\\g\\out_put"));
        job.waitForCompletion(true);
    }
}
