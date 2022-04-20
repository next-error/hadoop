package day05.Yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class demo01 {
    private static class Mapper01 extends Mapper<LongWritable, Text,Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int numReduceTasks = context.getNumReduceTasks();
            Text k2 = new Text();
            IntWritable v2 = new IntWritable();
            String[] split = value.toString().split("\\s+");
            for (String s : split) {
                k2.set(s+"-"+new Random().nextInt(numReduceTasks));
                v2.set(1);
                context.write(k2,v2);
            }

        }
    }
    private static class Reducer01 extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum =0;
            IntWritable v3 = new IntWritable();
            for (IntWritable value : values) {
                sum+=value.get();
            }
            v3.set(sum);
            context.write(key,v3);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration cof = new Configuration();
        //操作HDFS数据
        cof.set("fs.defaultFS","hdfs://linux01:8020");
        //设置运行模式
        cof.set("mapreduce.framework.name","yarn");
        //设置ResourceManger位置
        cof.set("yarn.resourcemanager.hostname","linux01");
        //设置MapReducer程序运行在Windows上的跨平台参数
        cof.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(cof, "join");
        job.setJar("D:\\IdeaProjects\\hadoop\\target\\test_yarn.jar");
        job.setMapperClass(Mapper01.class);
        job.setReducerClass(Reducer01.class);
        //map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //Re
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job,new Path("/hadoop_test/input"));
        FileOutputFormat.setOutputPath(job,new Path("/hadoop_test/output"));
        job.waitForCompletion(true);
    }
}
