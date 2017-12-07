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

public class map1 extends Mapper<Object, Text, Text, TextFloatPair> {

  private Text data = new Text();
  private HashMap<String, String> movies;


  public void map(Object key, Text data, Context context) throws IOException, InterruptedException {

    //Use this line for the test set (.csv)
    String[] fields = data.toString().split(",");
    
    //Use this line for the real dataset
    //String[] fields = data.toString().split("::");

    if (fields[0] != "userId" && fields.length == 4){
      String userID = fields[0];
      String movieID = fields[1];
      float rating = Float.parseFloat(fields[2]);

      Text outKey = new Text(userID);

      //Edited the IntIntPair class from the previous coursework to make TextFloatPair
      TextFloatPair outValue = new TextFloatPair(movieID, rating);

      //Output user ID as key and a pair as value, containing a movieID they have rated and their rating for that movie
      context.write(outKey, outValue);
    }
  }

  //Not sure we need this other database yet? movie ID is fine for now, we'll only need movie name later when making recommendations?

  // @Override
  // protected void setup(Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {

  //   movies = new HashMap<String, String>();

  //   URI fileUri = context.getCacheFiles()[0];

  //   FileSystem fs = FileSystem.get(context.getConfiguration());
  //   FSDataInputStream in = fs.open(new Path(fileUri));

  //   BufferedReader br = new BufferedReader(new InputStreamReader(in));

  //   String line = null;
  //   try {
  //     //discard the header row
  //     br.readLine();

  //     while ((line = br.readLine()) != null) {

  //       String[] fields = line.split("::");

  //       // 3 fields in the dataset, separated by a ::
  //       //field 1 is the movie ID, field 2 is the movie name
  //       if (fields.length == 3)
  //         movies.put(fields[0], fields[1]);
  //     }

  //     br.close();
  //   } catch (IOException e1) {
  //   } catch (ArrayIndexOutOfBoundsException e2){

  //   }

  //   super.setup(context);
  // }

}
