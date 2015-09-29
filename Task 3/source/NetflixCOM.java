import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class NetflixCOM {

	/**
	 * MAPPER
	 */
	static class MovieMapper extends TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable row, Result values, Context context) 
				throws IOException, InterruptedException {

			// Obtaining the row key from HBase
			ImmutableBytesWritable rowKey = 
					new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);

			String rowKeyStringValue = new String(rowKey.get(), "UTF-8");			
			String[] rowKeyItems = rowKeyStringValue.split(" ");
			String movieID = rowKeyItems[0];
			String reviewDate = rowKeyItems[1];
			/*System.out.println(movieID+" = "+reviewDate);*/
			context.write(new Text(movieID), new Text(reviewDate));
		}
	}


	/**
	 * REDUCER
	 */
	public static class MovieReducer extends
	Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			int openingStrength = 1;
			String movieID = key.toString();

			Iterator<Text> iter = values.iterator();
			String launchDateString = iter.next().toString();

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd");
			try {
				Date launchDate = dateFormat.parse(launchDateString);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(launchDate);
				calendar.add(Calendar.DATE, 30);
				Date closingDate = calendar.getTime();
				/*System.out.println("Launch Date: "+launchDate.toString());
				System.out.println("Closing Date: "+closingDate.toString());*/

				while(iter.hasNext()) {
					Date iteratedDate = dateFormat.parse(iter.next().toString());
					if(iteratedDate.before(closingDate)) {
						openingStrength++;
					}
				}
			} catch (ParseException e) {
				/*System.out.println("Parse exception parsing String to Date object");*/
				e.printStackTrace();
				System.exit(-1);
			}
			
			System.out.println("Opening strength for "+movieID+": "+openingStrength);
			context.write(new Text(movieID), new IntWritable(openingStrength));
		}
	}


	// MAIN
	public static void main(String[] args) throws Exception {

		// ******************** For Testing ************************************
		/*args = new String[1];
		args[0] = "/Users/frank/Courses/MapReduce/final_project/netflix_dataset/sample_op";
		File file = new File(args[0]);
		if (file.isDirectory()) {
			for (File f : file.listFiles())
				f.delete();
			file.delete();
		}*/
		// ***********************************************************************


		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "Netflix_COM");
		job.setJarByClass(NetflixCOM.class); // class that contains mapper
		String[] otherArgs =
				new GenericOptionsParser(config, args).getRemainingArgs();

		// Scanning the rows and only beginning from year 2008
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		// Intializing the mapper based on the rows in the HBase
		TableMapReduceUtil.initTableMapperJob("movie_reviews",
				scan,
				MovieMapper.class,
				Text.class,
				Text.class,
				job);

		job.setMapperClass(MovieMapper.class);
		/*job.setPartitionerClass(FlightsPartitioner.class);
			job.setSortComparatorClass(FlightMonthKeyComparator.class);
			job.setGroupingComparatorClass(FlightMonthKeyComparator.class);*/
		job.setReducerClass(MovieReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		job.waitForCompletion(true);

	}
}