import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexNetflix {

	public static class DataMapper 
	extends Mapper<Object, Text, Text, Text>{
		String movie_id;
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			//System.out.println("Umang's Idea");
			String itr = value.toString();
			
			String date;
			Integer rating;
			String customer_id;
			String year;

			if(value.toString().endsWith(":"))
			{  		
				String next_row = itr;
				movie_id = next_row.substring(0, next_row.length()-1);
			}else{
				//System.out.println("itr "+itr);
				String[] next_line = itr.split(",");
				customer_id=next_line[0];
				rating=Integer.parseInt(next_line[1]);
				date=next_line[2];
				String[] date_split= date.split("-");
				year=date_split[0];
				StringBuilder str =new StringBuilder();
				str.append("(");
				//str.append(movie_id);
				str.append(customer_id);
				str.append(","+year);
				str.append(","+rating+")");
				str.append(",");
				//context.write(new Text(customer_id), new Text(str.toString()));
				context.write(new Text(movie_id), new Text(str.toString()));
			}
		} 
	}

	public static class DataReducer 
	extends Reducer<Text,Text,Text,Text> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<Text> values, 
				Context context
				) throws IOException, InterruptedException {
			//System.out.println("Puneet's Idea");
			StringBuilder values_full = new StringBuilder("");
			String final_values="";
			for(Text t:values)
			{
				//System.out.println("Key: "+key+" value "+t);
				values_full.append(t);
				
			}
		//	final_values=values_full.substring(0, (values_full.length())-11);
			context.write(key, new Text(values_full.toString().substring(0,(values_full.toString().length()-1))));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(InvertedIndexNetflix.class);
		job.setMapperClass(DataMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
