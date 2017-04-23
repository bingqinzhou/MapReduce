package finalproject_try;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalSim2 {
	
	public static class SimMapper1 extends Mapper<Object, Text, Text, Text>{
		   
	    //Input:  customerId,movieId,rating
		//Output: key:  customerId 
		//        val:  movieId:rating
	   
	   private Text outKey = new Text();
	   private Text outValue = new Text();

	   public void map(Object key, Text value, Context context
	          ) throws IOException, InterruptedException {
		  
		  String[] line = value.toString().split(",");
	      String customerId = line[0];
	      String movieId = line[1];
	      String rating = line[2];
	    	  
	      outKey.set(customerId);
	      outValue.set(movieId+":"+rating);
	      context.write(outKey, outValue); 	               
	   }
	}
	
	public static class SimMapper2 extends Mapper<Object, Text, Text, Text>{
		   
	    //Input:  userId,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating4
		//Output: key:  customerId 
		//        val:  movieId:rating
	   
	   private Text outKey = new Text();
	   private Text outValue = new Text();

	   public void map(Object key, Text value, Context context
	          ) throws IOException, InterruptedException {
		  
		  String[] line = value.toString().split(",");
	      String userId = line[0];
	      String content = line[1]+","+line[2];
	    	  
	      outKey.set("U"+userId);
	      outValue.set(content);
	      context.write(outKey, outValue); 
	               
	   }
	}
	
	public static class SimReducer extends Reducer<Text,Text,Text,Text> {
		
		   //key: customerId, vals: [movieId:rating,movieId:rating..]
		   //U+user,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating4

		   //userId,customerId,sim,movie0Id+rating,movie1Id+rating;
		
		   ArrayList<String> users = new ArrayList<String> ();
		   Text outvalue = new Text();
		   		   
		   public void reduce(Text key, Iterable<Text> values,Context context
		                ) throws IOException, InterruptedException {
			   
			   if(key.toString().charAt(0) == 'U'){
				   String keyStr = key.toString();
				   String userId = keyStr.substring(1, keyStr.length());
				   for(Text val : values){
					   users.add(userId+","+val.toString());
				   }
			   }else{
				   String customerId = key.toString();
				   ArrayList<String> valList = new ArrayList<>();
				   for(Text val : values){
					   valList.add(val.toString());
				   }
				   
				   for(int i = 0; i < users.size(); i++){
					   String[] userLine = users.get(i).split(",");
					   String userId = userLine[0];
					   String[] predList = userLine[1].split("\\+");
					   String[] otherList = userLine[2].split("\\+");
					   String result = "";
					   boolean ifRatedCommonMovie = false;
					   float sim = 0;
					   float sqr_sum = 0;
					   for(String val:valList){
						   String[] c_pair = val.toString().split(":");
						   String c_movieId = c_pair[0];
						   String c_rating = c_pair[1];
						   for(int j = 0; j < predList.length; j++){
							   String[] predict_pair = predList[j].split(":");
							   String predict_movie_id = predict_pair[0];
							   String predict_movie_rating = predict_pair[1];
							   if(predict_movie_id.equals(c_movieId)){
								   ifRatedCommonMovie = true;
								   result = c_movieId+":"+c_rating+","+result;
								   float r1 = Float.parseFloat(c_rating);
								   float r2 = Float.parseFloat(predict_movie_rating);
								   float diff = r1-r2;
								   sqr_sum = new Double(sqr_sum+Math.pow(diff,2)).floatValue();
							   }
						   }
						   for(int j = 0; j < otherList.length; j++){
							   String[] u_pair = otherList[j].split(":");
							   String u_movieId = u_pair[0];
							   String u_rating = u_pair[1];
							   if(u_movieId.equals(c_movieId)){
								   ifRatedCommonMovie = true;
								   float r1 = Float.parseFloat(c_rating);
								   float r2 = Float.parseFloat(u_rating);
								   float diff = r1-r2;
								   sqr_sum = new Double(sqr_sum+Math.pow(diff,2)).floatValue();
							   }
						   }
					   }
					   if(ifRatedCommonMovie && !result.equals("")){
						   sim = 1/(1+sqr_sum);
						   result = userId+","+customerId+","+String.valueOf(sim)+","+result;
						   result = result.substring(0,result.length()-1);
						   outvalue.set(result);
						   context.write(null,outvalue);
					   }
				   }
			   }			   
		   }
		   
	}

	
	public static class SimPartitioner extends Partitioner<Text, Text>{
		
		@Override
	    public int getPartition(Text key, Text result, int numReduceTasks) {
			String keyStr = key.toString();
			if(keyStr.charAt(0) == 'U'){
				int userId = Integer.parseInt(keyStr.substring(1,keyStr.length()));
				return partitionUser(userId);
			}else{
				int customerId = Integer.parseInt(keyStr);
				return partitionCustomer(customerId);
			}
	    }  		 
		
	}
	
	private static int partitionCustomer(int customerId){
		
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
	
	private static int partitionUser(int userId){
		
		if(userId > 0 && userId <= 264943){
	    	return 1;
	    }else if(userId > 264943 && userId <= 264943*2){
	    	return 2;
	    }else if(userId > 264943*2 && userId <= 264943*3){
	    	return 3;
	    }else if(userId > 264943*3 && userId <= 264943*4){
	    	return 4;
	    }else if(userId > 264943*4 && userId <= 264943*5){
	    	return 5;
	    }else if(userId > 264943*5 && userId <= 264943*6){
	    	return 6;
	    }else if(userId > 264943*6 && userId <= 264943*7){
	    	return 7;
	    }else if(userId > 264943*7 && userId <= 264943*8){
	    	return 8;
	    }else if(userId > 264943*8 && userId <= 264943*9){
	    	return 9;
	    }else{
	    	return 0;
	    }
	}
	
	public static class SimKeyComparator extends WritableComparator {
	    protected SimKeyComparator() {
	      super(Text.class, true);
	    }
	    
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	      Text key1 = (Text) w1;
	      Text key2 = (Text) w2;
	      String keyStr1 = key1.toString();
	      String keyStr2 = key2.toString();
	      if(keyStr1.charAt(0) == 'U' && keyStr2.charAt(0) != 'U'){
	    	  return -1;
	      }else if(keyStr1.charAt(0) != 'U' && keyStr2.charAt(0) == 'U'){
	    	  return 1;
	      }else{
	    	  return keyStr1.compareTo(keyStr2);
	      }
	    }
	  }
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "calculate similarity");
	    job.setJarByClass(CalSim2.class);
	    
	    MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,SimMapper2.class);
	    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,SimMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,SimMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,SimMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[4]),TextInputFormat.class,SimMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[5]),TextInputFormat.class,SimMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[6]),TextInputFormat.class,SimMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[7]),TextInputFormat.class,SimMapper1.class);
	    
	    job.setPartitionerClass(SimPartitioner.class);
	    job.setSortComparatorClass(SimKeyComparator.class);
	    job.setNumReduceTasks(10);
	    job.setReducerClass(SimReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[8]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }


}
