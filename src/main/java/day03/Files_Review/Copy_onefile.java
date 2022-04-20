package day03.Files_Review;

import com.sun.tools.javac.util.List;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class Copy_onefile {
    //使用字节流复制一份文件
    public static void main(String[] args) throws IOException {
        FileInputStream in = new FileInputStream("d:\\duoyi\\韩顺平_2021图解Linux全面升级.pdf");
        FileOutputStream out = new FileOutputStream("d:\\g\\1.pdf");
        //使用数组读取
        byte[] bytes = new byte[1024];
        int read = in.read(bytes);
        //System.out.println(read);
        java.util.List<String> list = new ArrayList<>();

    }
}
