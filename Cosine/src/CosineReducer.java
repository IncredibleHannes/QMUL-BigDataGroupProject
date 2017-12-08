package coursework2;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosineReducer extends Reducer<Text, RatingInfo, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<RatingInfo> values, Context context)
        throws IOException, InterruptedException {
        HashMap<String, HashMap<String, Float>> usersRatings = new HashMap<String, HashMap<String, Float>>();
        HashMap<String, Float> moviesRankings;

        for (RatingInfo value : values) {
            String movieId = value.getMovieId().toString();
            String userId = value.getUserId().toString();
            Float rating = value.getRating().get();

            moviesRankings = usersRatings.get(userId);
            if (moviesRankings == null) {
                moviesRankings = new HashMap<String, Float>();
            }
            moviesRankings.put(movieId, rating);
            usersRatings.put(userId, moviesRankings);
        }

        String[] movies = key.toString().split("\\|");
        Set<String> userKeys = usersRatings.keySet();
        float num = 0;
        float dem1 = 0;
        float dem2 = 0;
        for (String userKey : userKeys) {
            moviesRankings = usersRatings.get(userKey);
            num += moviesRankings.get(movies[0])*moviesRankings.get(movies[1]);
            dem1 += Math.pow(moviesRankings.get(movies[0]), 2);
            dem2 += Math.pow(moviesRankings.get(movies[1]), 2);
        }

        double similarity = 0;
        if (!movies[0].equals(movies[1])) {
          similarity = num / (Math.sqrt(dem1)*Math.sqrt(dem2));
        }

        context.write(key, new DoubleWritable(similarity));
    }
}
