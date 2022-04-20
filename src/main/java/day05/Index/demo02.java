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

public class demo02 {
    private static class Index02Mappper extends Mapper<LongWritable, Text, Text, Text> {
        String fileName;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fs = (FileSplit) inputSplit;
            fileName = fs.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text k2 = new Text();
            Text v2 = new Text();
            String kk = value.toString().split("-")[0];
            String vv = value.toString().split("-")[1].replaceAll("\\s+", "-");
            k2.set(kk);
            v2.set(vv);
            context.write(k2, v2);
        }
    }

    private static class Index02Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text v3 = new Text();
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                String s = sb.append(" " + value).toString();
                v3.set(s);
                context.write(key, v3);
            }
        }


        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
            Configuration cof = new Configuration();
            Job job = Job.getInstance(cof, "join");
            job.setMapperClass(Index02Mappper.class);
            job.setReducerClass(Index02Reducer.class);
            //map
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //Re
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job, new Path("d:\\g\\out_put"));
            FileOutputFormat.setOutputPath(job, new Path("d:\\g\\out_put01"));
            job.waitForCompletion(true);
        }
    }
}
