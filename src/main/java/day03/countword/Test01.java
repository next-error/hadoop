package day03.countword;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Test01 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration con = new Configuration();
        Job job = Job.getInstance(con,"name");
        job.setMapperClass(Countmapper.class);
        job.setReducerClass(Countreduce.class);
        //map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce 输出类型
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(IntWritable.class);
        //文件位置
        FileInputFormat.setInputPaths(job,new Path("d:\\g\\a123.txt"));
        //
        FileOutputFormat.setOutputPath(job,new Path("d:\\g\\bbb\\e"));
        //提交任务 并等待任务结束
        job.waitForCompletion(true);
    }
}
