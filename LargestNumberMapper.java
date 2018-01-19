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

  public class LargestNumberMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static Text one = new Text();
    private IntWritable num = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      one.set("oneCount");
      while (itr.hasMoreTokens()) {
        num.set(Integer.parseInt(itr.nextToken()));
        context.write(one, num);
       
    }
        
  }
   
}
