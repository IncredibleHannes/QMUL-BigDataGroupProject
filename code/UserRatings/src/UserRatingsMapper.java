package coursework2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.Normalizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UserRatingsMapper extends Mapper<Object, Text, Text, TextFloatPair> {

  private Text data = new Text();

  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
    //Use this line for the test set (.csv)
    //String[] fields = data.toString().split(",");

    //Use this line for the real dataset
    String[] fields = data.toString().split("::");

    if (fields[0] != "userId" && fields.length == 4){
      String userID = fields[0];
      String movieID = fields[1];
      float rating = Float.parseFloat(fields[2]);

      Text outKey = new Text(userID);

      //Edited the IntIntPair class from the previous coursework to make TextFloatPair
      TextFloatPair outValue = new TextFloatPair(movieID, rating);

      //Output user ID as key and a pair as value, containing a movieID they have rated and their rating for that movie
      context.write(outKey, outValue);
    }
  }

}
