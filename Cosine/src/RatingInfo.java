package coursework2;

import java.io.*;
import org.apache.hadoop.io.*;

public class RatingInfo implements Writable {

   private Text movieId;
   private Text userId;
   private FloatWritable rating;

   public RatingInfo() {
      this.movieId = new Text();
      this.userId = new Text();
      this.rating = new FloatWritable();
   }

   public RatingInfo(String movieId, String userId, float rating) {
      set(movieId, userId, rating);
   }

   public void set(String movieId, String userId, float rating) {
      this.movieId = new Text(movieId);
      this.userId = new Text(userId);
      this.rating = new FloatWritable(rating);
   }

   public Text getMovieId() {
      return movieId;
   }

   public Text getUserId() {
      return userId;
   }

   public FloatWritable getRating() {
      return rating;
   }

   public void write(DataOutput out) throws IOException {
      movieId.write(out);
      userId.write(out);
      rating.write(out);
   }

   public void readFields(DataInput in) throws IOException {
      movieId.readFields(in);
      userId.readFields(in);
      rating.readFields(in);
   }

   @Override
   public int hashCode() {
      return movieId.hashCode() * 163 + userId.hashCode() * 30 + rating.hashCode();
   }

   public boolean equals(Object o) {
      if (o instanceof RatingInfo) {
         RatingInfo ratingInfo = (RatingInfo) o;
         return rating.equals(ratingInfo.rating) && movieId.equals(ratingInfo.movieId) && userId.equals(ratingInfo.userId);
      }
      return false;
   }

   public String toString() {
      return movieId.toString() + "\t" + userId.toString() + "\t" + rating.toString();
   }

   public int compareTo(RatingInfo ratingInfo) {
      int cmp = movieId.compareTo(ratingInfo.movieId);
      if (cmp != 0) {
         return cmp;
      }
      cmp = userId.compareTo(ratingInfo.userId);
      if (cmp != 0) {
         return cmp;
      }
      return rating.compareTo(ratingInfo.rating);
   }
}
