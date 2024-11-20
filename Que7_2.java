import org.apache.hadoop.io.LongWritable;
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

public class Que7_2 {
	public class MapperJoin extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final IntWritable one = new IntWritable(1);
	    private Text outputKey = new Text("Documentary 1995");

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] fields = value.toString().split(",");

	        if (fields.length > 3 && fields[2].contains("Documentary") && fields[3].equals("1995")) {
	            context.write(outputKey, one);
	        }
	    }
	}
	
	public class RedcuerJoin extends Reducer<Text, IntWritable, Text, IntWritable> {
	    private IntWritable result = new IntWritable();

	    @Override
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        result.set(sum);
	        context.write(key, result);
	    }
	}

	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: Documentary1995Job <input path> <output path>");
	            System.exit(-1);
	        }

	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Documentary Movies 1995");
	        job.setJarByClass(Que7_2.class);
	        job.setMapperClass(MapperJoin.class);
	        job.setReducerClass(RedcuerJoin.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
