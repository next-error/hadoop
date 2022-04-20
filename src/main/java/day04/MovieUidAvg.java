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
import java.util.HashSet;
import java.util.Set;

public class MovieUidAvg {
    private static class MovieMapper extends Mapper<LongWritable, Text,Text,Movie>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Movie>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Movie movie = JSON.parseObject(line, Movie.class);
            Text k2 = new Text();
            k2.set(movie.getMovie());
            context.write(k2,movie);
        }
    }
    private static class MovieReducer extends Reducer<Text,Movie,Text,Text>{
        double sum =0;
        int count =0;
        Text v3 = new Text();
        Set<String> set = new HashSet<>();
        @Override
        protected void reduce(Text key, Iterable<Movie> values, Reducer<Text, Movie, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Movie value : values) {
                sum+=value.getRate();
                count++;
                set.add(value.getUid());

            }
            v3.set("平均分:"+sum/count+"评分人数:"+set.size());
            context.write(key,v3);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof, "movie");
        job.setMapperClass(MovieUidAvg.MovieMapper.class);
        job.setReducerClass(MovieUidAvg.MovieReducer.class);
        //设置mapper输出类型
        job.setMapOutputValueClass(Movie.class);
        job.setMapOutputKeyClass(Text.class);
        //设置Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("d:\\g\\movie.txt"));
        FileOutputFormat.setOutputPath(job,new Path("d:\\g\\out_put1"));
        job.waitForCompletion(true);
    }
}
