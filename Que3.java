import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Que3 {

	public class MapperJoin extends  Mapper<Object, Text, Text, IntWritable>{
		private final IntWritable one = new IntWritable(1);
	    private Text word = new Text();
		
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] tokens = line.split("\\s+");
	        for (String token : tokens) {
	            word.set(token);
	            context.write(word, one);
	        }
	    }
		
	}
	
	public class ReducerJoin extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private Map<String, Integer> tokenCounts = new HashMap<>();
	    private int totalTokens = 0;
	    private int uniqueTokens = 0;

	    @Override
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        tokenCounts.put(key.toString(), sum);
	        totalTokens += sum;
	        uniqueTokens++;
	    }

	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        for (Map.Entry<String, Integer> entry : tokenCounts.entrySet()) {
	            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	        }
	        double averageCount = (double) totalTokens / uniqueTokens;
	        context.write(new Text("AverageCount"), new IntWritable((int) Math.round(averageCount)));
	    }
		

	}

	public static void main(String[] args) throws Exception {
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"que-1");
		
		
		job.setJarByClass(Que3.class);
		MultipleInputs.addInputPath(job, input, TextInputFormat.class, MapperJoin.class);
		job.setReducerClass(ReducerJoin.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true)?0:1);
	
	}
}
