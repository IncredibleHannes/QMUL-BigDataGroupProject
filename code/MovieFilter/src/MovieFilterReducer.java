package coursework2;

import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import javafx.util.Pair;
import java.util.Collections.*;
import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieFilterReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        ArrayList<Pair<String,Float>> movieSimilarity = new ArrayList<Pair<String,Float>>();
        for (Text value : values) {
          String[] valueFields = value.toString().split(":");
          Float similarity = Float.parseFloat(valueFields[1]);
          String movie1 = valueFields[0];
          movieSimilarity.add(new Pair(movie1, similarity));
        }

        movieSimilarity.sort(new Comparator<Pair<String, Float>>() {
           @Override
           public int compare(Pair<String, Float> o1, Pair<String, Float> o2) {
               if (o1.getValue() > o2.getValue()) {
                   return -1;
               } else if (o1.getValue().equals(o2.getValue())) {
                   return 0;
               } else {
                   return 1;
               }
           }
        });
        if (movieSimilarity.size() > 10) {
          for(int i = 0; i < 10; i++){
            Pair<String,Float> p = movieSimilarity.get(i);
            context.write(new Text(key.toString() + "," + p.getKey()), new DoubleWritable(p.getValue()));
          }
        } else {
          for(Pair<String,Float> p : movieSimilarity) {
            context.write(new Text(key.toString() + "," + p.getKey()), new DoubleWritable(p.getValue()));
          }
        }
    }
}
