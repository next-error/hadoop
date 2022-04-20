package day02;

import day02.HDFSUtils.hdfsUilts;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class demo02 {
    public static void main(String[] args) throws IOException {
        FileSystem fs = hdfsUilts.GetFileSystem();
        Path path = new Path("/");
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus file : fileStatuses) {
     //       System.out.println(file);
            //获取访问时间
            System.out.println("访问时间:"+file.getAccessTime());
            //获取块的大小
            System.out.println("块的大小"+file.getBlockSize());
            ////获得组名
            System.out.println("组名:"+file.getGroup());
            //文件大小
            System.out.println("文件大小:"+file.getLen());
            //修改时间
            System.out.println("最后修改时间"+file.getModificationTime());
            //所有者信息
            System.out.println("所有者:"+file.getOwner());
            //获取Path路径
            System.out.println("路径:"+file.getPath());
            //获取权限信息
            System.out.println("权限信息:"+file.getPermission());
            //获取块副本数
            System.out.println("块副本数"+file.getReplication());
            //获取符号链接的Path路径
           // System.out.println("符号链接的Path路径:"+file.getSymlink());
            //判断是否是符号链接
            System.out.println("是否是符号链接:"+file.isSymlink());



        }
        fs.close();
    }
}
