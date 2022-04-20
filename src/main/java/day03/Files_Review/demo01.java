package day03.Files_Review;

import java.io.File;
import java.io.FileFilter;

public class demo01 {
    public static void main(String[] args) {
        //检索出某一目录下所有以.java结尾的文件
        File file = new File("d:\\duoyi");
        File[] files = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.isDirectory()) {
                    return true;
                }
                    return pathname.getName().toLowerCase().endsWith(".java");


            }
        });
        for (File file1 : files) {
            System.out.println(file1);
        }
    }
}
