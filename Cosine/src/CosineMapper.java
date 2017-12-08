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

public class CosineMapper extends Mapper<Object, Text, Text, RatingInfo> {

  private Text data = new Text();
  private ArrayList<String> movies;

  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
    String[] fields = data.toString().split("\t");

    String userId = fields[0];
    String[] ratings = fields[1].split("\\|");

    HashMap<String, Float> finalRatings = new HashMap<String, Float>();

    float sum = 0;
    int count = 0;
    for (int i = 0; i < ratings.length; i++) {
      String[] movieRatingPair = ratings[i].split(",");
      finalRatings.put(movieRatingPair[0], Float.parseFloat(movieRatingPair[1]));
      sum += Float.parseFloat(movieRatingPair[1]);
      count++;
    }

    float average = sum / count;

    for (int i = 0; i < movies.size(); i++) {
      for (int j = 0; j < movies.size(); j++) {
        if (finalRatings.containsKey(movies.get(i)) && finalRatings.containsKey(movies.get(j))) {
          float movie1 = finalRatings.get(movies.get(i));
          float movie2 = finalRatings.get(movies.get(j));

          int compare = movies.get(i).compareTo(movies.get(j));
          String moviesPair = "";
          if (compare <= 0) {
            moviesPair = movies.get(i) + "|" + movies.get(j);
          } else {
            moviesPair = movies.get(j) + "|" + movies.get(i);
          }
          Float num = movie1 - average;
          context.write(new Text(moviesPair), new RatingInfo(movies.get(i), userId, num));

        }
      }
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {

    movies = new ArrayList<String>();

    URI moviesUri = context.getCacheFiles()[0];

    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream in = fs.open(new Path(moviesUri));

    BufferedReader br = new BufferedReader(new InputStreamReader(in));

    String line = null;
    try {

      while ((line = br.readLine()) != null) {
          String[] fields = line.split("::");
          if (fields.length == 3) {
            movies.add(fields[0]);
            //movies.add(line);
          }
      }

      br.close();
    } catch (IOException e1) {
    } catch (ArrayIndexOutOfBoundsException e2){

    }

    super.setup(context);
  }

}
