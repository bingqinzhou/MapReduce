package finalproject_try;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class FormatUserInput {
	
	//input:  customer,movie,rating;
	//input:  user,movie1,movie2;
	//output: user,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating
	
	public static class InputMapper extends Mapper<Object, Text, Text, Text>{
		   private Text outKey = new Text();
		   private Text outValue = new Text();

		   public void map(Object key, Text value, Context context
		          ) throws IOException, InterruptedException {
			  
			  String[] line = value.toString().split(",");
		      String customer = line[0];
		      String movie = line[1];
		      String rating = line[2];
		      outKey.set(customer);
		      outValue.set(movie+":"+rating);
		      context.write(outKey, outValue);	               
		   }
	}
	
	public static class InputReducer extends Reducer<Text,Text,Text,Text> {
		//key:customer, values:[movie1:rating1,movie2:rating2..]
		//user,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating4
	    
		Text outValue = new Text();
		File usersFile;
		HashMap<String,String> users = new HashMap<> ();
		 
		//user,movie1,movie2
		//key:user value:movie1+movie2
		public void setup(Context context) throws IOException, InterruptedException{
			   if (context.getCacheFiles() != null && context.getCacheFiles().length > 0){
				   URI userFileUri = context.getCacheFiles()[0];
		           usersFile = new File(userFileUri.toString());
			   }
			   try {
				   BufferedReader reader = new BufferedReader(new FileReader(usersFile));
		           String line;
		           while  ((line = reader.readLine()) != null){
		              String[] lineArr =line.split(",");
		              String user = lineArr[0];
		              String movies = "";
		              for(int i = 1; i < lineArr.length; i++){
		            	  movies = lineArr[i]+"+"+movies;
		              }
		              if(!movies.equals("")){
		            	  movies = movies.substring(0,movies.length()-1);
		              }
		              users.put(user, movies);
		           }
		           reader.close();
		       } catch (IOException x){
		           System.err.format("IOException: %s%n", x);
		       }
		 }
		 
		 public void reduce(Text key, Iterable<Text> values,Context context
	                ) throws IOException, InterruptedException {
			 String customer = key.toString();
			 String predict_movies = "";
			 String other_movies = "";
			 if(users.containsKey(customer)){
				 String[] movies_predict = users.get(customer).split("\\+");
				 for(Text val : values){
					 String[] pair = val.toString().split(":");
					 String movie = pair[0];
					 String rating = pair[1];
					 boolean if_predict_movie = false;
					 for(int i = 0; i < movies_predict.length; i++){
						 String movie_predict = movies_predict[i];
						 if(movie.equals(movie_predict)){
							 if_predict_movie = true;
							 predict_movies = movie+":"+rating+"+"+predict_movies;
							 break;
						 }
					 }
					 if(!if_predict_movie){
						 other_movies = movie+":"+rating+"+"+other_movies;
					 }
				 }
				 if(!predict_movies.equals("")){
					 predict_movies = predict_movies.substring(0,predict_movies.length()-1);
				 }
				 if(!other_movies.equals("")){
					 other_movies = other_movies.substring(0,other_movies.length()-1);
				 }
				 outValue.set(customer+","+predict_movies+","+other_movies);
				 context.write(null, outValue);
			 }
		 }
	}
	
	public static class InputPartitioner extends Partitioner<Text, Text>{
		
		@Override
	    public int getPartition(Text key, Text result, int numReduceTasks) {
			int customerId = Integer.parseInt(key.toString());
		    if(customerId > 0 && customerId <= 264943){
		    	return 0;
		    }else if(customerId > 264943 && customerId <= 264943*2){
		    	return 1;
		    }else if(customerId > 264943*2 && customerId <= 264943*3){
		    	return 2;
		    }else if(customerId > 264943*3 && customerId <= 264943*4){
		    	return 3;
		    }else if(customerId > 264943*4 && customerId <= 264943*5){
		    	return 4;
		    }else if(customerId > 264943*5 && customerId <= 264943*6){
		    	return 5;
		    }else if(customerId > 264943*6 && customerId <= 264943*7){
		    	return 6;
		    }else if(customerId > 264943*7 && customerId <= 264943*8){
		    	return 7;
		    }else if(customerId > 264943*8 && customerId <= 264943*9){
		    	return 8;
		    }else{
		    	return 9;
		    }
	    }  		
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "formate input");
	    job.setJarByClass(FormatUserInput.class);
	    
	    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,InputMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,InputMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,InputMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[4]),TextInputFormat.class,InputMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[5]),TextInputFormat.class,InputMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[6]),TextInputFormat.class,InputMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[7]),TextInputFormat.class,InputMapper.class);

	    job.setPartitionerClass(InputPartitioner.class);
	    job.setNumReduceTasks(10);
	    job.setReducerClass(InputReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.addCacheFile(new Path(args[0]).toUri());
	    FileOutputFormat.setOutputPath(job, new Path(args[8]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
