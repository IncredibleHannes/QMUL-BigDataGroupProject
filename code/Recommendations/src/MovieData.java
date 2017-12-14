package coursework2;

import java.io.*;
import org.apache.hadoop.io.*;

public class MovieData implements Writable {

   private DoubleWritable similarity;
   private DoubleWritable userNormRating;
   private DoubleWritable userMinRating;
   private DoubleWritable userMaxRating;

   public MovieData() {
      this.similarity = new DoubleWritable();
      this.userNormRating = new DoubleWritable();
      this.userMinRating = new DoubleWritable();
      this.userMaxRating = new DoubleWritable();
   }

   public MovieData(double similarity, double userNormRating, double userMinRating, double userMaxRating) {
      set(similarity, userNormRating, userMinRating, userMaxRating);
   }

   public void set(double similarity, double userNormRating, double userMinRating, double userMaxRating) {
     this.similarity = new DoubleWritable(similarity);
     this.userNormRating = new DoubleWritable(userNormRating);
     this.userMinRating = new DoubleWritable(userMinRating);
     this.userMaxRating = new DoubleWritable(userMaxRating);
   }

   public DoubleWritable getSimilarity() {
      return similarity;
   }

   public DoubleWritable getUserNormRating() {
      return userNormRating;
   }

   public DoubleWritable getUserMinRating() {
      return userMinRating;
   }

   public DoubleWritable getUserMaxRating() {
      return userMaxRating;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      similarity.write(out);
      userNormRating.write(out);
      userMinRating.write(out);
      userMaxRating.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      similarity.readFields(in);
      userNormRating.readFields(in);
      userMinRating.readFields(in);
      userMaxRating.readFields(in);
   }

   @Override
   public int hashCode() {
      return similarity.hashCode() * 150 + userNormRating.hashCode();
   }

   public boolean equals(Object o) {
      if (o instanceof MovieData) {
         MovieData md = (MovieData) o;
         return similarity.equals(md.similarity) && userNormRating.equals(md.userNormRating) && userMinRating.equals(md.userMinRating) && userMaxRating.equals(md.userMaxRating);
      }
      return false;
   }

   public String toString() {
      return similarity.toString() + "\t" + userNormRating.toString() + "\t" + userMinRating.toString() + "\t" + userMaxRating.toString();
   }

   public int compareTo(MovieData md) {
      return 0;
   }
}
