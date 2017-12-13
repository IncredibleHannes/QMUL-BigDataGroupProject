package coursework2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.Normalizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class JaccardMapper extends Mapper<Object, Text, Text, Text> {

  private Text data = new Text();

  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
    String[] fields = data.toString().split("\t");

    String userId = fields[0];
    String[] pairs = fields[1].split("\\|");

    ArrayList<String> movies = new ArrayList<String>();
    ArrayList<Float> ratings = new ArrayList<Float>();

    for (int i = 0; i < pairs.length; i++) {
      String[] movieRatingPair = pairs[i].split(",");
      movies.add(movieRatingPair[0]);
      ratings.add(Float.parseFloat(movieRatingPair[1]));
    }

    int moviesSize = movies.size();
    for (int i = 0; i < moviesSize; i++) {
      for (int j = i; j < moviesSize; j++) {
        String movie1 = movies.get(i);
        String movie2 = movies.get(j);
        context.write(new Text(movies.get(i) + "," + movies.get(j)), new Text(ratings.get(i)+ "," +ratings.get(j)));
      }
    }
  }
}
