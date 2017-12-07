package coursework2;

import java.io.*;
import org.apache.hadoop.io.*;

public class TextFloatPair implements WritableComparable<TextFloatPair> {

   private Text first;
   private FloatWritable second;

   public TextFloatPair() {
      this.first = new Text();
      this.second = new FloatWritable();
   }

   public TextFloatPair(String first, float second) {
      set(first, second);
   }

   public void set(String first, float second) {
      this.first = new Text(first);
      this.second = new FloatWritable(second);
   }

   public Text getFirst() {
      return first;
   }

   public FloatWritable getSecond() {
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
      return first.hashCode() * 163 + second.hashCode();
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof TextFloatPair) {
         TextFloatPair tp = (TextFloatPair) o;
         return second.equals(tp.second);
      }
      return false;
   }

   @Override
   public String toString() {
      return first.toString() + "\t" + second.toString();
   }

   @Override
   public int compareTo(TextFloatPair tp) {
      int cmp = first.compareTo(tp.first);
      if (cmp != 0) {
         return cmp;
      }
      return second.compareTo(tp.second);
   }
}