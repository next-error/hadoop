package day02;

import day02.HDFSUtils.hdfsUilts;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class demo01 {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://linux01:8020");
        Configuration con = new Configuration();
        FileSystem fs = FileSystem.newInstance(uri, con, "root");
        fs.mkdirs(new Path("/bbb"));
        fs.close();
        FileSystem fs1 = hdfsUilts.GetFileSystem();
        Path path = new Path("d:\\g\\aaa\\a.txt");
        Path dst = new Path("/aaa/1.txt");
/*        fs1.copyFromLocalFile(path,dst);
        fs1.close();*/
        Path path1 = new Path("/aaa/1.txt");
        Path dst1 = new Path("d:\\g\\aaa\\bbb");
        FileSystem fs2 = hdfsUilts.GetFileSystem();
        fs2.copyToLocalFile(path1,dst1);
        fs2.close();
    }

}
