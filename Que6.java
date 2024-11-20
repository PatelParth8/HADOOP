import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Que6 {
	public class MapperJoin extends Mapper<LongWritable, Text, Text, IntWritable> {

	    private final IntWritable one = new IntWritable(1);
	    private Text userId = new Text();

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        // Split the line by commas
	        String[] fields = value.toString().split(",");

	        // Check if the record has the expected number of fields
	        if (fields.length > 1) {
	            String user = fields[0].trim(); // Assuming user ID is the first field
	            String category = fields[1].trim(); // Assuming category is the second field

	            // Check if the review is for Musical Instruments
	            if (category.equalsIgnoreCase("Musical Instruments")) {
	                userId.set(user);
	                context.write(userId, one); // Emit user ID with count 1
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
	        context.write(key, result); // Emit user ID and total count
	    }
	}

	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: ReviewCountJob <input path> <output path>");
	            System.exit(-1);
	        }

	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Review Count for Musical Instruments");
	        job.setJarByClass(Que6.class);
	        job.setMapperClass(MapperJoin.class);
	        job.setCombinerClass(ReducerJoin.class); // Optional: can use Combiner for optimization
	        job.setReducerClass(ReducerJoin.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
