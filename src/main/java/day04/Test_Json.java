package day04;

import com.alibaba.fastjson.JSON;

import java.io.*;

public class Test_Json {
    public static void main(String[] args) throws IOException {
        BufferedReader bfr = new BufferedReader(new FileReader("d:\\g\\movie.txt"));
        String line =null;
        while ((line=bfr.readLine()) != null) {
            Movie movie = JSON.parseObject(line, Movie.class);
            System.out.println(movie);
        }



    }
}
