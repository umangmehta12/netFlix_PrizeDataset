package Project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class netflixProject {
	
	
	public static class DoublePair 
	implements WritableComparable<DoublePair> {
		private double first = 0;
		private double second = 0;

		/**
		 * Set the left and right values.
		 */
		public void set(double left, double right) {
			first = left;
			second = right;
		}
		public double getFirst() {
			return first;
		}
		public double getSecond() {
			return second;
		}
		/**
		 * Read the two integers. 
		 * Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE, MAX_VALUE-> -1
		 */
		@Override
	    public void readFields(DataInput in) throws IOException {
	        // YOUR CODE HERE
	        first = in.readDouble();
	        second = in.readDouble();

	    }
		@Override
	    public void write(DataOutput out) throws IOException {
	        // YOUR CODE HERE
	        out.writeDouble(first);
	        out.writeDouble(second);

	    }
		@Override
	    public int compareTo(DoublePair o) {
	      if (first != o.first) {
	        return first < o.first ? -1 : 1;
	      } else if (second != o.second) {
	        return second < o.second ? 1 : -1;
	      } else {
	        return 0;
	      }
	    }


	}
	

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	double rating_sum;
	double num_of_ratings;
	double average_rating;
	String movie_id;
	
	public void setup(){
		rating_sum = 0.0;
		num_of_ratings = 0.0;
		average_rating = 0.0;
	}
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String itr = value.toString();
    	if(value.toString().endsWith(":"))
    	{   		
    		String next_row = itr;
    		movie_id = next_row.substring(0, next_row.length()-1);
    	}else{
			String[] next_line = itr.split(",");
			//System.out.println(next_line[0]+" "+next_line[1]+" "+next_line[2]);
			average_rating = rating_sum / num_of_ratings;
			rating_sum += Double.parseDouble(next_line[1]);
			num_of_ratings++;	
    	}
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException{
    	context.write(new Text(movie_id), new DoubleWritable(average_rating));
    }
  }//End of TokenizerMapper
  
  
  
  public static class MovieAverageRatingMapper 
  extends Mapper<Object, Text, Text, Text>{
 
		public void map(Object key, Text value, Context context
		               ) throws IOException, InterruptedException {
			
			String[] each_row = value.toString().split("\\s+");
		    context.write(new Text(each_row[0]), new Text("Average file"+","+each_row[1]));//"Average File"+avearge rating
		
		}
  }
  
  public static class MovieFileMapper 
  extends Mapper<Object, Text, Text, Text>{
 
		public void map(Object key, Text value, Context context
		               ) throws IOException, InterruptedException {
			
			String[] each_row = value.toString().split(",");
		    context.write(new Text(each_row[0]), new Text("Movie file"+","+each_row[1]+","+each_row[2])); //"Movie File"+Year+Movie Name
		
		}
  }
  
	public static class MovieJoinReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> AverageFileList = new ArrayList<Text>();
		private ArrayList<Text> MovieFileList = new ArrayList<Text>();
		private String joinType;

		@Override
		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
            //System.out.println("Coming inside Reducer");
			// Clear our lists
			AverageFileList.clear();
			MovieFileList.clear();

			for (Text t : values) {
				String [] value_list = t.toString().split(",");
				if (value_list[0].equals("Average file")) {
					//System.out.println("LegOneList");
					AverageFileList.add(new Text(t.toString()));
				} else if (value_list[0].equals("Movie file")) {
					//System.out.println("LegTwoList");
					MovieFileList.add(new Text(t.toString()));
				}
			}

			int z = 0;
			// Execute our join logic now that the lists are filled
			executeJoinLogic(context,key);
		}
		
        private void executeJoinLogic(Context context, Text key) throws IOException,
                    InterruptedException{
        	//System.out.println("Coming to executeJoinLogic");
        	if(joinType.equalsIgnoreCase(joinType)){
        		//System.out.println("Coming insode jpin");
        		if(!AverageFileList.isEmpty() && !MovieFileList.isEmpty()) {
        			//System.out.println("List not e,ty");System.out.println("LegOneList");
        			for(Text legone: AverageFileList){
        				for(Text legtwo:MovieFileList){
        					//System.out.println("Coming to Loop");
        					String[] leg_one_values = legone.toString().split(",");
        					//System.out.println(leg_one_values[0]+" "+leg_one_values[1]+" "+leg_one_values[2]+" "+leg_one_values[3]+" "+leg_one_values[4]+" "+leg_one_values[5]);
        					String[] leg_two_values = legtwo.toString().split(",");
        					//System.out.println(leg_two_values[0]+" "+leg_two_values[1]+" "+leg_two_values[2]+" "+leg_two_values[3]+" "+leg_two_values[4]+" "+leg_two_values[5]);
        					context.write(null,new Text(key+"@"+leg_one_values[1] + "@"+leg_two_values[1]+"@"+leg_two_values[2]));

        				}
        			}
        		}
        	}
        	
        }
		
	}
	
	
	//Mapper and reducer jobs to get the top 5 movies
	  public static class TopMoviesMapper 
      extends Mapper<Object, Text, DoublePair, Text>{
     
		   public void map(Object key, Text value, Context context
		                   ) throws IOException, InterruptedException {
			   
			   //Split values to get movie id and rating,year,movie_name
			   String[] each_line = value.toString().split("@");
			   
			   DoublePair key_value=new DoublePair();
			   //Key will be year,rating
			   if(each_line[2]!=null && each_line[1]!=null && !each_line[2].equals("NULL") && !each_line[1].equals("NULL")){
				   key_value.set(Double.parseDouble(each_line[2]), Double.parseDouble(each_line[1]));
				   context.write(key_value, new Text(each_line[0]+"@"+each_line[3]));
				 }

		    
		   }
	  }
	  
	  public static class KeyComparator extends WritableComparator {
		    protected KeyComparator() {
		      super(DoublePair.class, true);
		    }
		    @Override
		    public int compare(WritableComparable w1, WritableComparable w2) {
		      DoublePair ip1 = (DoublePair) w1;
		      DoublePair ip2 = (DoublePair) w2;
		      int cmp;
		      
		      if(ip1.getFirst() == ip2.getFirst()){
		    	  cmp =0;
		      }else if(ip1.getFirst() > ip2.getFirst()){
		    	  cmp = 1;
		      }else{
		    	  cmp = -1;
		      }
		      if (cmp != 0) {
		        return cmp;
		      }
		      
		      if(ip1.getSecond() == ip2.getSecond()){
		    	  cmp =0;
		      }else if(ip1.getSecond() > ip2.getSecond()){
		    	  cmp = -1;
		      }else{
		    	  cmp = 1;
		      }
		      
		      return cmp; //reverse
		    }
		  }

	  public static class GroupComparator extends WritableComparator {
		    protected GroupComparator() {
		      super(DoublePair.class, true);
		    }
		    @Override
		    public int compare(WritableComparable w1, WritableComparable w2) {
		    	DoublePair ip1 = (DoublePair) w1;
		    	DoublePair ip2 = (DoublePair) w2;
		      int cmp;
		      
		      if(ip1.getFirst() == ip2.getFirst() && ip1.getSecond() == ip2.getSecond()){ 

		    	  cmp = 0;
		      }else if(ip1.getFirst() > ip2.getFirst() ){

		    	  cmp = 1;
		      }else{

		    	  cmp = -1;
		      }
		      return cmp;
		    }
		  }
	  
	  
	  public static class YearPartitioner extends Partitioner<Object,Text> {
			@Override
			public int getPartition(Object key, Text value, int numPartitions) {
				
				DoublePair val = (DoublePair)key;
				System.out.println("numPartitions:"+numPartitions);
				Double year = val.getFirst();
				if(year <1920){
					return 0;
				}
				else if(year>=1920 && year <1940){
					return 1;
				}
				else if(year>=1941 && year <1960){
					return 2;
				}
				else if(year>=1961 && year <1980){
					return 3;
				}
				else if(year>=1981 && year <2000){
					return 4;
				}
				else if(year>=2000 && year <2020){
					return 5;
				}
				
				return -1;
				
			}
		 
		}
	  
	  
	  public static class TopMoviesReducer
	    extends Reducer<DoublePair, Text, Text, Text> {
		  
		  	
		  	HashMap<Double,Integer> each_year_count = new HashMap<Double,Integer>();
		    @Override
		    protected void reduce(DoublePair key, Iterable<Text> values,
		        Context context) throws IOException, InterruptedException {
		    	
		    	String[] value_list ={} ;
		    	for (Text val : values) {
			        value_list = val.toString().split("@");
			     }
		       
		    	if(each_year_count.containsKey(key.getFirst())){
		    		if(each_year_count.get(key.getFirst()) <=5){
		    			context.write(null,new Text((int)key.getFirst()+" "+key.getSecond()+" "+value_list[0]+" "+value_list[1]));
		    			each_year_count.put(key.getFirst(), each_year_count.get(key.getFirst())+1);
		    		}
		    	}else{
		    		context.write(null,new Text((int)key.getFirst()+" "+key.getSecond()+" "+value_list[0]+" "+value_list[1]));
	    			each_year_count.put(key.getFirst(), 1);
		    	}
		       //Output year,rating,movie ID, movie name 	
		       
		       
		       
		    }
	  }  
	  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 5) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(netflixProject.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.waitForCompletion(true);
    
    //Second Mapreduce Job to join the average movie ratings file to the movie file
    Job job1 = new Job(conf, "join with movie file");
    job1.setJarByClass(netflixProject.class);
    job1.getConfiguration().set("join.type", "inner");
	MultipleInputs.addInputPath(job1, new Path(otherArgs[1]),
			TextInputFormat.class, MovieAverageRatingMapper.class);

	MultipleInputs.addInputPath(job1, new Path(otherArgs[2]),
			TextInputFormat.class, MovieFileMapper.class);
    job1.setReducerClass(MovieJoinReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]));
    job1.waitForCompletion(true);	 
    
    //Job to find top 5 movies each year
    Job job2 = new Job(conf, "find top 5 movies");
    job2.setJarByClass(netflixProject.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
    job2.setPartitionerClass(YearPartitioner.class);
    job2.setNumReduceTasks(5);
    job2.setMapperClass(TopMoviesMapper.class);
    job2.setMapOutputKeyClass(DoublePair.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setReducerClass(TopMoviesReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    //job1.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[4]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    
  }
}
