package day02;

import day02.HDFSUtils.hdfsUilts;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class demo04 {
    public static void main(String[] args) throws IOException {
        FileSystem fs = hdfsUilts.GetFileSystem();
        FSDataOutputStream out = fs.create(new Path("/aaa/2.txt"),true);
        out.writeUTF("西湖\r\n");
        out.writeUTF("痛仰\r\n");
        out.writeUTF("没有理想的人不伤心\r\n");
        out.close();
        FSDataOutputStream append = fs.append(new Path("/aaa/2.txt"));
        append.writeUTF("新裤子\r\n");
        append.writeUTF("刺猬\r\n");
        append.close();

        FSDataInputStream in = fs.open(new Path("/aaa/2.txt"));
        BufferedReader buff = new BufferedReader(new InputStreamReader(in));
       // String s = buff.readLine();
        String s =null;
        while((s = buff.readLine())!=null){

            System.out.println(s);
        }
        in.close();
        fs.close();

    }
}
