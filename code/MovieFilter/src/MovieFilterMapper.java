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
import java.util.Hashtable;
import java.util.Enumeration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MovieFilterMapper extends Mapper<Object, Text, Text, Text> {

  ArrayList<Integer> movies;

  String targetUser = "100";
  Hashtable<Integer, Double> userRatings;
  double userMaxRating;
  double userMinRating;

  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
    String[] fields = data.toString().split("\t");

    String[] moviesPair = fields[0].split(",");
    Float similarity = Float.parseFloat(fields[1]);

    int movie1 = Integer.parseInt(moviesPair[0]);
    int movie2 = Integer.parseInt(moviesPair[1]);

    if (userRatings.get(movie1) != null && userRatings.get(movie2) == null) {
      context.write(new Text(movie1 + ""), new Text(movie2 + ":" + fields[1]));
    } else if (userRatings.get(movie1) == null && userRatings.get(movie2) != null) {
      context.write(new Text(movie2 + ""), new Text(movie1 + ":" + fields[1]));
    }
  }

  @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		movies = new ArrayList<Integer>();
    userRatings = new Hashtable<Integer, Double>();

		// Movies file
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("::");
        //String[] fields = line.split("\t");
				// Fields are: 0:Id 1:Movie Title
				movies.add(Integer.parseInt(fields[0]));
			}
			br.close();
		} catch (IOException e1) {
		}

    // User Ratings
    fileUri = context.getCacheFiles()[1];

		fs = FileSystem.get(context.getConfiguration());
		in = fs.open(new Path(fileUri));

		br = new BufferedReader(new InputStreamReader(in));

		line = null;
    userMaxRating = (double)0.0;
    userMinRating = (double)Integer.MAX_VALUE;
		try {
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\t");
				// Fields are: 0:UserId 1:RatingPairs

        String userId = fields[0];
        if (userId.compareTo(targetUser) == 0) {
          String[] pairs = fields[1].split("\\|");

          for (int i = 0; i < pairs.length; i++) {
            String[] movieRatingPair = pairs[i].split(",");
            double rating = Float.parseFloat(movieRatingPair[1]);
            userRatings.put(Integer.parseInt(movieRatingPair[0]), rating);
            if (rating > userMaxRating) {
              userMaxRating = rating;
            }
            if (rating < userMinRating) {
              userMinRating = rating;
            }
          }
        }
			}

			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
