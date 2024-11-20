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

public class Que5 {
	public class MapperJoin extends Mapper<LongWritable, Text, Text, IntWritable> {

	    private final IntWritable one = new IntWritable(1);
	    private Text female = new Text("female");

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        // Split the line by commas
	        String[] fields = value.toString().split(",");

	        // Check if the record has the expected number of fields
	        if (fields.length == 4) {
	            String gender = fields[2].trim(); // Gender is the third field
	            if (gender.equalsIgnoreCase("female")) {
	                context.write(female, one); // Emit "female" with count 1
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
	        context.write(new Text("No. of female voters are : "), result); // Emit the total count
	    }
	}

	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: FemaleVoterCountJob <input path> <output path>");
	            System.exit(-1);
	        }

	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Female Voter Count");
	        job.setJarByClass(Que5.class);
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
