package day05.Join;

import day04.MovieAvg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.ArrayList;

public class demo01 {
    private static class JoinMapper extends Mapper<LongWritable, Text,Text,Text>{
        String filename;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fs = (FileSplit) inputSplit;
            filename = fs.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String uid =null;
            Text k2 = new Text();
            if (filename.equals("order.txr")) {
                 uid = value.toString().split("\\s+")[1];

            }else {
                 uid = value.toString().split(",")[0];
            }
            k2.set(uid);
            context.write(k2,value);
        }
    }
    private static class JoinReducer extends Reducer<Text,Text,Text, NullWritable>{
        String user=null;
        ArrayList<String> list = new ArrayList();
        Text k3 =new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (value.toString().contains(",")) {
                    user = value.toString();
                } else {
                    list.add(value.toString().split("\\s+")[0]);

                }

                for (String s : list) {
                    k3.set(s + " " + user);
                    context.write(k3, NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof, "movie");
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("d:\\g\\input"));
        FileOutputFormat.setOutputPath(job,new Path("d:\\g\\out_put8"));
        job.waitForCompletion(true);
    }
}
