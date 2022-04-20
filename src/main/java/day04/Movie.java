package day04;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Movie implements Writable {
    private String movie;
    private double rate;
    private long timeStamp;
    private  String uid;

    public Movie() {
    }

    public Movie(String movie, double rate, long timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp=" + timeStamp +
                ", uid='" + uid + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeDouble(rate);
        dataOutput.writeLong(timeStamp);
        dataOutput.writeUTF(uid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       movie = dataInput.readUTF();
       rate = dataInput.readDouble();
       timeStamp = dataInput.readLong();
       uid = dataInput.readUTF();
    }
}
