package day02.HDFSUtils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
public class hdfsUilts {
    static FileSystem fs;
    public static FileSystem GetFileSystem(){
        try {
            URI uri = new URI("hdfs://linux01:8020");
            Configuration con = new Configuration();
             fs = FileSystem.newInstance(uri, con, "root");
            return fs;
        }
        catch (Exception e){
            throw new RuntimeException() ;
        }

    }
    public static void HDFSclose(){
        try {
            fs.close();
        }
        catch (Exception e){
            System.out.println(e);
        }

    }


}
