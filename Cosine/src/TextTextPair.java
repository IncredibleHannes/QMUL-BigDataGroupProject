package coursework2;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextTextPair implements WritableComparable<TextTextPair> {

   private Text first;
   private Text second;

   public TextTextPair() {
      this.first = new Text();
      this.second = new Text();
   }

   public TextTextPair(String first, String second) {
      set(first, second);
   }

   public void set(String first, String second) {
      this.first = new Text(first);
      this.second = new Text(second);
   }

   public Text getFirst() {
      return first;
   }

   public Text getSecond() {
      return second;
   }

   @Override
   public void write(DataOutput out) throws IOException {
      first.write(out);
      second.write(out);
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      first.readFields(in);
      second.readFields(in);
   }

   @Override
   public int hashCode() {
      return first.hashCode() * 150 + second.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof TextTextPair) {
         TextTextPair tp = (TextTextPair) o;
         return first.equals(tp.first) && second.equals(tp.second);
      }
      return false;
   }

   @Override
   public String toString() {
      return first.toString() + "\t" + second.toString();
   }

   @Override
   public int compareTo(TextTextPair tp) {
      int cmp = first.compareTo(tp.first);
      if (cmp != 0) {
         return cmp;
      }
      return second.compareTo(tp.second);
   }
}
