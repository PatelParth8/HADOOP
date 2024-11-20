import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Que4 {
		public class MapperJoin extends Mapper<Object, Text, Text, IntWritable> {

	    private final IntWritable one = new IntWritable(1);
	    private Text token = new Text();

	    @Override
	    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        // Split the line into tokens
	        String[] tokens = value.toString().split("\\W+");
	        
	        // Loop through tokens
	        for (String str : tokens) {
	            if (str.length() >= 4) { // Check length condition
	                token.set(str); // Set the token
	                context.write(token, one); // Emit the token with count 1
	            }
	        }
	    }
	}	

		public class ReducerJoin extends Reducer<Text, IntWritable, Text, IntWritable> {

		    private IntWritable result = new IntWritable();

		    @Override
		    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		        int sum = 0;
		        for (IntWritable val : values) {
		            sum += val.get(); // Sum the counts
		        }
		        result.set(sum);
		        context.write(key, result); // Emit the token and its count
		    }
		}

		    public static void main(String[] args) throws Exception {
		        if (args.length != 2) {
		            System.err.println("Usage: TokenLengthJob <input path> <output path>");
		            System.exit(-1);
		        }

		        Configuration conf = new Configuration();
		        Job job = Job.getInstance(conf, "Token Length Count");
		        job.setJarByClass(Que4.class);
		        job.setMapperClass(MapperJoin.class);
		        job.setCombinerClass(ReducerJoin.class);
		        job.setReducerClass(ReducerJoin.class);
		        job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(IntWritable.class);
		        FileInputFormat.addInputPath(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		        System.exit(job.waitForCompletion(true) ? 0 : 1);
		    }
		}