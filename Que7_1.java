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

public class Que7_1 {
	public class MapperJoin extends Mapper<LongWritable, Text, Text, Text> {
	    private Text outputKey = new Text("Comedy Movie");
	    private Text outputValue = new Text();

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] fields = value.toString().split(",");

	        if (fields.length > 2 && fields[2].contains("Comedy")) {
	            outputValue.set(value);
	            context.write(outputKey, outputValue);
	        }
	    }
	}
	
	public class ReducerJoin extends Reducer<Text, Text, Text, Text> {
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for (Text val : values) {
	            context.write(key, val);
	        }
	    }
	}

	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: ComedyMoviesJob <input path> <output path>");
	            System.exit(-1);
	        }

	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Comedy Movies");
	        job.setJarByClass(Que7_1.class);
	        job.setMapperClass(MapperJoin.class);
	        job.setReducerClass(ReducerJoin.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }

}
