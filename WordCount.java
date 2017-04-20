package hadoop;

import java.io.IOException;
// import java.util.StringTokenizer;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	// Mapper Class
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		private Set<String> skipWords;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			skipWords = new HashSet<String>();

			for (String word : conf.get("skip.words").split("\\s+")) {
				skipWords.add(word);
			}
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split("[^a-zA-Z]+");
			for (int i = 0; i < strs.length; i++) {
				if (skipWords.contains(strs[i])) continue;
				word.set(strs[i]);
				context.write(word, ONE);
			}
		}
	}
	// Reducer Class
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		class Node implements Comparable<Node> {
			Text key;
			Integer val;
			public Node(Text k, Integer v) {
				key = new Text(k);
				val = v;
			}
			public int compareTo(Node that) {
				return that.val - this.val;
			}
		}

		private TreeSet<Node> set = new TreeSet<Node>();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			set.add(new Node(key, sum));

			if (set.size() > 100) {
				set.pollLast();
			}
		}

		@Override
		protected void cleanup(Context context) 
			throws IOException, InterruptedException {
			for (Node n : set) {
				context.write(n.key, new IntWritable(n.val));
			}
		}
	
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("skip.words", "I V X");
		Job job = new Job(conf, "word count");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
