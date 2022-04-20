package day03.Files_Review;

import java.io.File;
import java.io.FileFilter;

public class demo_111 {
    public static void main(String[] args){
/*        File dir = new File("d:\\g");
        File[] files = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                //判断如果获取到的是目录，直接放行
                if(pathname.isDirectory())
                    return true;
                //获取路径中的文件名，判断是否java结尾，是就返回true
                return pathname.getName().toLowerCase().endsWith("txt");
            }
        });
        for(File file : files){
            System.out.println(file);
        }*/
        File dir = new File("d:\\duoyi");
        method(dir);

    }
    public static void  method(File dir){

        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                method(file);
            }
            if (file.getName().toLowerCase().endsWith("java")) {
                System.out.println(file);
            }
        }


    }
}
