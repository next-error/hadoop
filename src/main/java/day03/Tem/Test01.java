package day03.Tem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Test01 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //创建job对象
        Configuration con = new Configuration();
        Job job = Job.getInstance(con, "Max_Tem");
        //
        job.setMapperClass(Tem_Mapper.class);
        job.setReducerClass(Tem_Reduce.class);
        //输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //设置源文件及结果集文件路径
        FileInputFormat.setInputPaths(job,new Path("d:\\g\\tem.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:\\g\\Tem"));
        //开启job
        job.waitForCompletion(true);
    }
}
