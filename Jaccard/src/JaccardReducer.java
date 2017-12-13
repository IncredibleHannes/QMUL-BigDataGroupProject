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

public class JaccardReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        //For each movie pair, store the ratings for both movies in arrays
        ArrayList<Float> movie1 = new ArrayList<Float>();
        ArrayList<Float> movie2 = new ArrayList<Float>();
        
        for (Text t : values){
          String [] ratings = t.toString().split(",");
          movie1.add(Float.parseFloat(ratings[0]));
          movie2.add(Float.parseFloat(ratings[1]));
        }
        
        //Compare ratings of both movies and count how many times they have recieved the same rating and how many times they 
        //have recieved different ratings
        int sameRatings = 0;
        int diffRatings = 0;

        for (int i = 0; i < movie1.size(); i++){
          for (int j = i; j < movie2.size(); j++){
            if (movie1.get(i).equals(movie2.get(j)))
              sameRatings++;
            else
              diffRatings++;
          }

        double jaccard = sameRatings/diffRatings;

        context.write(key, new DoubleWritable(jaccard));
        }
    }
}
