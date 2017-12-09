package coursework2;

import java.io.*;
import org.apache.hadoop.io.*;

public class RatingInfo implements Writable {

   private FloatWritable rating1;
   private FloatWritable rating2;
   private FloatWritable average;

   public RatingInfo() {
      this.rating1 = new FloatWritable();
      this.rating2 = new FloatWritable();
      this.average = new FloatWritable();
   }

   public RatingInfo(float rating1, float rating2, float average) {
      set(rating1, rating2, average);
   }

   public void set(float rating1, float rating2, float average) {
      this.rating1 = new FloatWritable(rating1);
      this.rating2 = new FloatWritable(rating2);
      this.average = new FloatWritable(average);
   }

   public FloatWritable getRating1() {
      return rating1;
   }

   public FloatWritable getRating2() {
      return rating2;
   }

   public FloatWritable getAverage() {
      return average;
   }

   public void write(DataOutput out) throws IOException {
      rating1.write(out);
      rating2.write(out);
      average.write(out);
   }

   public void readFields(DataInput in) throws IOException {
      rating1.readFields(in);
      rating2.readFields(in);
      average.readFields(in);
   }

   @Override
   public int hashCode() {
      return rating1.hashCode() * 163 + rating2.hashCode() * 30 + average.hashCode();
   }

   public boolean equals(Object o) {
      if (o instanceof RatingInfo) {
         RatingInfo ratingInfo = (RatingInfo) o;
         return rating1.equals(ratingInfo.rating1) && rating2.equals(ratingInfo.rating2) && average.equals(ratingInfo.average);
      }
      return false;
   }

   public String toString() {
      return rating1.toString() + "\t" + rating2.toString() + "\t" + average.toString();
   }

   public int compareTo(RatingInfo ratingInfo) {
      int cmp = rating1.compareTo(ratingInfo.rating1);
      if (cmp != 0) {
         return cmp;
      }
      cmp = rating2.compareTo(ratingInfo.rating2);
      if (cmp != 0) {
         return cmp;
      }
      return average.compareTo(ratingInfo.average);
   }
}
