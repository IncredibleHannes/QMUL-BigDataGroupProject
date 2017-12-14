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

public class RecommendationsReducer extends Reducer<Text, Text, IntWritable, DoubleWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        String[] fields = key.toString().split(",");
        int movieId = Integer.parseInt(fields[0]);
        double min = Double.parseDouble(fields[1]);
        double max = Double.parseDouble(fields[2]);

        double num = 0;
        double dem = 0;
        for (Text value : values) {
          String[] valueFields = value.toString().split(",");
          double similarity = Double.parseDouble(valueFields[0]);
          double userNormRating = Double.parseDouble(valueFields[1]);
          num += similarity*userNormRating;
          dem += Math.abs(similarity);
        }

        double normPrediction = num / dem;
        double prediction = 0.5*(normPrediction + 1)*(max - min) + min;

        context.write(new IntWritable(movieId), new DoubleWritable(prediction));
    }
}
