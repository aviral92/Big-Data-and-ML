
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class LargestNumberReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable max = new IntWritable(Integer.MIN_VALUE);

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      for (IntWritable val : values) {
        if(max.get()<val.get())
          max.set(val.get());
      }
      context.write(new Text("Max is "), max);
    }
  }
