import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AlgoMapReduce extends Configured implements Tool {

	public static String header = "";

	public static class InstanceCountMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {

			String[] splits = value.toString().split("\t");
			String solver = splits[0];
			if (!solver.equals("Solver")) {
				String status = splits[5];
				Double time = Double.parseDouble(splits[11]);
				if (status.equals("complete")) {
					context.write(new Text(solver + "\t" + time), new DoubleWritable(time));
				}

			}
		}
	}

	public static class InstanceCountReduce extends Reducer<Text, DoubleWritable, LongWritable, Text> {

		private static String current = "";
		private static int count = 0;

		@Override
		protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,
				Reducer<Text, DoubleWritable, LongWritable, Text>.Context arg2)
				throws IOException, InterruptedException {

			String[] splittedKey = arg0.toString().split("\t");
			if (!current.equals(splittedKey[0])) {
				current = splittedKey[0];
				header += current + "\t";
				count = 0;
			}

			String value = splittedKey[0] + "\t";
			for (DoubleWritable doubleWritable : arg1) {
				count++;
				value = splittedKey[0] + "\t" + Double.toString(doubleWritable.get());
				arg2.write(new LongWritable(count), new Text(value));

			}

		}
	}

	public static class InstanceCountSort extends WritableComparator {
		public InstanceCountSort() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text t_a = (Text) a;
			Text t_b = (Text) b;

			String[] split_a = t_a.toString().split("\t");
			String[] split_b = t_b.toString().split("\t");
			String s_a = split_a[0];
			String s_b = split_b[0];
			if (s_a.compareTo(s_b) != 0)
				return s_a.compareTo(s_b);
			Double d_a = Double.parseDouble(split_a[1]);
			Double d_b = Double.parseDouble(split_b[1]);
			return d_a.compareTo(d_b);
		}

	}

	public static class InstanceMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new LongWritable(Integer.parseInt(split[0])), new Text(split[1] + "\t" + split[2]));
		}

	}

	public static class InstanceReduce extends Reducer<LongWritable, Text, Text, Text> {

		@Override
		protected void setup(Reducer<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new Text("n_instance"), new Text(header));
		}

		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1,
				Reducer<LongWritable, Text, Text, Text>.Context arg2) throws IOException, InterruptedException {
			String times = "";
			Map<String, String> map = new HashMap<>();

			for (Text val : arg1) {
				String[] splitted = val.toString().split("\t");
				map.put(splitted[0], splitted[1]);
			}
			for (String s : map.values()) {
				times += s + "\t";
			}
			String index = arg0.toString();
			arg2.write(new Text(index), new Text(times));
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		String[] argv = new GenericOptionsParser(conf, arg0).getRemainingArgs();

		Job job = Job.getInstance(conf, "AlgoMapReduce");
		job.setJarByClass(AlgoMapReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(InstanceCountMapper.class);
		job.setReducerClass(InstanceCountReduce.class);
		job.setSortComparatorClass(InstanceCountSort.class);

		ControlledJob controlledJob1 = new ControlledJob(conf);
		controlledJob1.setJob(job);

		JobControl jobController = new JobControl("jobControll");
		jobController.addJob(controlledJob1);

		Job job2 = Job.getInstance(conf, "AlgoMapReduceTwo");
		job2.setJarByClass(AlgoMapReduce.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(InstanceMapper.class);
		job2.setReducerClass(InstanceReduce.class);

		ControlledJob controlledJob2 = new ControlledJob(conf);
		controlledJob2.setJob(job2);
		controlledJob2.addDependingJob(controlledJob1);

		jobController.addJob(controlledJob2);

		FileInputFormat.setInputPaths(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));
		FileInputFormat.setInputPaths(job2, new Path(argv[2]));
		FileOutputFormat.setOutputPath(job2, new Path(argv[3]));

		new Thread(jobController).start();
		while (!jobController.allFinished())
			Thread.sleep(100);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AlgoMapReduce(), args);
		System.exit(res);
	}

}
