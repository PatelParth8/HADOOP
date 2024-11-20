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

public class Que7_4 {
	public class MapperJoin extends Mapper<LongWritable, Text, Text, Text> {
	    private Text outputValue = new Text();

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] fields = value.toString().split(",");
	        if (fields.length > 1 && fields[1].contains("Gold")) {
	            outputValue.set(fields[1]); // Set the title
	            context.write(outputValue, new Text("")); // Emit title
	        }
	    }
	}

	public class RedcuerJoin extends Reducer<Text, Text, Text, Text> {
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        context.write(key, new Text("")); // Just output the title
	    }
	}

	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: GoldTitleJob <input path> <output path>");
	            System.exit(-1);
	        }

	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Gold Title Movies");
	        job.setJarByClass(Que7_4.class);
	        job.setMapperClass(MapperJoin.class);
	        job.setReducerClass(RedcuerJoin.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
