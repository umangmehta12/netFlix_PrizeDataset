import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

public class kMeans {
	public static class DataMapper 
	extends Mapper<Object, Text, Text, Text>{
		
		//Hashmap of cluster ID and its related movie list in another hashmap
		public HashMap<Integer, HashMap<Integer,String>> cluster;
		//HashMap<Integer,Double> movie;
		double[] kClusters=new double[5];
		public void setup(Context context)
		{
			kClusters[0]=1;
			kClusters[1]=2;
			kClusters[2]=3;
			kClusters[3]=4;
			kClusters[4]=5;
			cluster= new HashMap<Integer,HashMap<Integer,String>>();
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			double rateAvg=0;
			String[]itr = value.toString().split("@");
			String name=itr[3];
			Integer movie_id=Integer.parseInt(itr[0]);
			Double rating=Double.parseDouble(itr[1]);
			
			List<Double> mod = new ArrayList<Double>();
			//Calculating the movie rating - cluster assigned rating
			//Find which cluster the movie belongs to
			for(double d:kClusters)
			{
			mod.add(Math.abs(rating-d));
			}
			int index =mod.indexOf(Collections.min(mod));
			double minimum=kClusters[index];
			
			//Put the movie into the cluster
			HashMap<Integer,String> movieDetails;
			if(cluster.get(index) != null){
				movieDetails=cluster.get(index);
			}else{
				movieDetails= new HashMap<Integer,String>();
			}
			StringBuilder details=new StringBuilder();
			details.append(rating);
			details.append(",");
			details.append(name);
			movieDetails.put(movie_id, details.toString());
			cluster.put(index,movieDetails); 
			
			
			//Now calculate the average of the cluster
			movieDetails = cluster.get(index);
			
			Iterator itrRating = movieDetails.entrySet().iterator();
			int count=0;
			while(itrRating.hasNext()){
				Map.Entry pairs = (Map.Entry)itrRating.next();
				String[] getrating=pairs.getValue().toString().split(",");
				rateAvg+=Double.parseDouble(getrating[0]);
				count++;
			}
			double calculate=rateAvg/count;
			rateAvg=calculate;
			kClusters[index]=rateAvg;
  
		}

		public void cleanup(Context context) throws IOException, InterruptedException
		{
		for(Integer cId : cluster.keySet())
	    {
	        for(Integer movieId : cluster.get(cId).keySet())
	        {   
	        	context.write(new Text(cId.toString()),new Text(cluster.get(cId).get(movieId)));
	        }
	    }
			    		    
		}
		}
	
	public static class DataReducer 
		extends Reducer<Text,Text,Text,Text> {
			private IntWritable result = new IntWritable();
			public void reduce(Text key, Iterable<Text> values, 
					Context context
					) throws IOException, InterruptedException {
				StringBuilder values_full = new StringBuilder("");
				String final_values="";
				for(Text t:values)
				{ String[] val=t.toString().split(",");
					values_full.append(val[1]);
					values_full.append('\n');
				}
				StringBuilder keys=new StringBuilder();
				keys.append(key);
				keys.append('\n');
				context.write(new Text(keys.toString()), new Text(values_full.toString().substring(0,(values_full.toString().length()-1))));
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "K-Means");
		job.setJarByClass(kMeans.class);
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