package coursework2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingsReducer extends Reducer<Text, TextFloatPair, Text, Text> {

    public void reduce(Text key, Iterable<TextFloatPair> values, Context context)
        throws IOException, InterruptedException {
        String outRatings = "";

        for (TextFloatPair value : values) {
            outRatings += value.getFirst() + "," + value.getSecond() + "|";
        }
        // Remove last separator | from the output String
        outRatings = outRatings.substring(0, outRatings.length() - 1);

        context.write(key, new Text(outRatings));
    }
}
