package day05.Index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class demo01 {
    private static class Index01Mappper extends Mapper<LongWritable, Text,Text, IntWritable>{
        String fileName;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fs = (FileSplit) inputSplit;
            fileName = fs.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            Text k2 =new Text();
            IntWritable v2 = new IntWritable();
            String[] split = value.toString().split("\\s+");
            StringBuilder sb = new StringBuilder();
            for (String s : split) {

                String s1 = sb.append(s.toString()).append("-").append(fileName).toString();
                k2.set(s1);
                v2.set(1);
                context.write(k2,v2);
            }
        }
    }
    private static class Index01Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum =0;
            IntWritable v3 = new IntWritable();
            for (IntWritable value : values) {
                sum++;
            }
            v3.set(sum);
            context.write(key,v3);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof, "join");
        job.setMapperClass(Index01Mappper.class);
        job.setReducerClass(Index01Reducer.class);
        //map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //Re
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("d:\\g\\input01"));
        FileOutputFormat.setOutputPath(job,new Path("d:\\g\\out_put"));
        job.waitForCompletion(true);
    }
}
