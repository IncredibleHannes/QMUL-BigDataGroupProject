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

public class CosineReducer extends Reducer<Text, TextFloatPair, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<TextFloatPair> values, Context context)
        throws IOException, InterruptedException {
        HashMap<String, HashMap<String, Float>> usersRatings = new HashMap<String, HashMap<String, Float>>();

        for (TextFloatPair value : values) {
            String textKey = value.getFirst().toString();
            String[] fields = textKey.split("\\|");
            if (usersRatings.containsKey(fields[1])) {
                HashMap<String, Float> oldMovieRank = usersRatings.get(fields[1]);
                oldMovieRank.put(fields[0], value.getSecond().get());
                usersRatings.put(fields[1], oldMovieRank);
            } else {
                HashMap movieRank = new HashMap<String, Float>();
                movieRank.put(fields[0], value.getSecond().get());
                usersRatings.put(fields[1], movieRank);
            }
        }

        String[] movies = key.toString().split("\\|");
        Set<String> userKeys = usersRatings.keySet();
        float num = 0;
        float dem1 = 0;
        float dem2 = 0;
        for (String userKey : userKeys) {
            HashMap<String, Float> moviesRankings = usersRatings.get(userKey);
            num += moviesRankings.get(movies[0])*moviesRankings.get(movies[1]);
            dem1 += Math.pow(moviesRankings.get(movies[0]), 2);
            dem2 += Math.pow(moviesRankings.get(movies[1]), 2);
        }

        context.write(key, new DoubleWritable(num / (Math.sqrt(dem1)*Math.sqrt(dem2))));
    }
}
