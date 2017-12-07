package coursework2;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.Normalizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class map1 extends Mapper<Object, Text, Text, IntWritable> {

  private final IntWritable one = new IntWritable(1);
  private Text data = new Text();
  private HashMap<String, String> movies;


  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {

    String[] fields = data.toString().split("::");

    if (fields[0] == "userId")
      return;

    String userID = fields[0];
    String movieID = fields[1];
    String rating = fields[2];

    data.set(movieID);
    context.write(data, one);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {

    movies = new HashMap<String, String>();

    URI fileUri = context.getCacheFiles()[0];

    FileSystem fs = FileSystem.get(context.getConfiguration());
    FSDataInputStream in = fs.open(new Path(fileUri));

    BufferedReader br = new BufferedReader(new InputStreamReader(in));

    String line = null;
    try {
      //discard the header row
      br.readLine();

      while ((line = br.readLine()) != null) {

        String[] fields = line.split("::");

        // 11 fields in the dataset, separated by a comma
        //field 2 is the medalist name, field 8 is the sport
        if (fields.length == 3)
          movies.put(fields[0], fields[1]);
      }

      br.close();
    } catch (IOException e1) {
    } catch (ArrayIndexOutOfBoundsException e2){

    }

    super.setup(context);
  }

}
