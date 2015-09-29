import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import au.com.bytecode.opencsv.CSVParser;


public class NetflixPOP {
	
	public static Configuration hBaseConfig = HBaseConfiguration.create();
	public static HTableDescriptor ht;
	public static HTable hTable;
	
	
	/**
	 * MAPPER
	 */
	public static class MovieMapper extends
	Mapper<Object, Text, NullWritable, NullWritable> {
		String currentMovieID;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// Creating new table named flight_log
			hTable = new HTable(hBaseConfig, "movie_reviews"); 
			hTable.setAutoFlush(false);
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			// Closing the table once mapping is complete
			hTable.close();
		}


		//		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//			super.map(key, value, context);

			
			String valueString = value.toString();
			
			// Parsing the movie ID
			if(valueString.endsWith(":")) {
				currentMovieID = valueString.substring(0, valueString.indexOf(":")); 
				System.out.println("Current movie ID: "+currentMovieID);
			}
			else {
				// Parse movie id, review date and customer id from value rowID = “movieID reviewDate customerID”
				String values[] = valueString.split(",");
				String customerID = values[0];
				String rating = values[1];
				String reviewDate = values[2];
				
				String rowID = currentMovieID+" "+reviewDate+" "+customerID;
				
				// Add row to HTable
				// Add rating to column family “info”
				Put p = new Put(Bytes.toBytes(rowID));					
				p.add(Bytes.toBytes("info"), 
						Bytes.toBytes("rating"), 
						Bytes.toBytes(rating));

				// Adding columns to table
				hTable.put(p);
			}
			context.write(NullWritable.get(), NullWritable.get());
		}
	}

	
	public static void main(String[] args) throws Exception{

		// ******************** For Testing ************************************
		/*args = new String[2];
		args[0] = "/Users/frank/Courses/MapReduce/final_project/netflix_dataset/small_training_set";
		args[1] = "/Users/frank/Courses/MapReduce/final_project/netflix_dataset/sample_op";
		File file = new File(args[1]);
		if (file.isDirectory()) {
			for (File f : file.listFiles())
				f.delete();
			file.delete();
		}
		File inputDir = new File(args[0]);
		if(inputDir.isDirectory()) {
			for(File f : inputDir.listFiles()) {
				if(f.getName().equalsIgnoreCase(".DS_Store")) {
					f.delete();
				}
			}
		}*/
		// ***********************************************************************
		
		// Checking if the table already exists before attempting to create it
		HBaseAdmin hbAdmin = new HBaseAdmin(hBaseConfig);
		if(!hbAdmin.tableExists("movie_reviews")) {
			ht = new HTableDescriptor("movie_reviews");
			ht.addFamily(new HColumnDescriptor("info"));
			hbAdmin.createTable(ht);
		}
		hbAdmin.close();


		Configuration conf = new Configuration();
		String[] otherArgs =
				new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err
			.println("Require two inputs: <in file> <non-existing op folder>");
			System.exit(2);
		}
		Job job = new Job(conf, "Netflix_POP");
		job.setJarByClass(NetflixPOP.class);
		job.setMapperClass(MovieMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		String uriStr = otherArgs[0]; 
		URI uri = URI.create(uriStr); 
		FileSystem fs = FileSystem.get(uri, conf);
		Path cFile = new Path(args[0]);
		
	    // FileStatus list from given dir
	    FileStatus[] statusList = fs.listStatus(cFile);
	    if(statusList != null){
	        for(FileStatus status : statusList){
	            // Adding each file to the list of inputs for the map-reduce job
	            FileInputFormat.addInputPath(job, status.getPath());
	        }
	    }
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
