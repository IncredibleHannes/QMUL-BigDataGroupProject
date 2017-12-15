package coursework2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;

public class CosineMapper extends Mapper<Object, Text, Text, Text> {

  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
    String[] fields = data.toString().split("\t");

    String userId = fields[0];
    String[] pairs = fields[1].split("\\|"); // pairs of movies and ratings which user made

    // movies and corresponding ratings
    ArrayList<Integer> movies = new ArrayList<Integer>();
    ArrayList<Float> ratings = new ArrayList<Float>();

    // create movies and ratings collections
    // and calculate sum and count for adjusted Cosine similarity metric
    float sum = 0;
    int count = 0;
    for (int i = 0; i < pairs.length; i++) {
      String[] movieRatingPair = pairs[i].split(",");
      movies.add(Integer.parseInt(movieRatingPair[0]));
      ratings.add(Float.parseFloat(movieRatingPair[1]));
      sum += Float.parseFloat(movieRatingPair[1]);
      count++;
    }

    // average rating of user (for adjusted Cosine similarity metric)
    float average = sum / count;

    // emit pairs of movies and corresponding ratings
    // (movie1, movie2) -> (rating1, rating2)
    int moviesSize = movies.size();
    for (int i = 0; i < moviesSize; i++) {
      for (int j = i; j < moviesSize; j++) {
        if (movies.get(i) > movies.get(j)) { // to remove duplicate keys: (movie1, movie2) and (movie2, movie1) should be the same key
          context.write(new Text(movies.get(i)+","+ movies.get(j)), new Text(ratings.get(i)+","+ratings.get(j)+","+ average));
        } else {
          context.write(new Text(movies.get(j)+","+ movies.get(i)), new Text(ratings.get(j)+","+ratings.get(i)+","+ average));
        }
      }
    }
  }
}
