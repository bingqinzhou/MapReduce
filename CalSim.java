package finalproject_try;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;


public class CalSim {
	
public static class SimMapper extends Mapper<Object, Text, Text, Text>{
   
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


public static class SimReducer extends Reducer<Text,Text,Text,Text> {
	
   //key: customerId, vals: [movieId:rating,movieId:rating..]
   //userId,customerId,sim,movie0Id+rating,movie1Id+rating;
	
   Text outValue = new Text();
   File usersFile;
   URI userFileUri;
   static ArrayList<String> users;
   
   //user,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating4
   public void setup(Context context) throws IOException, InterruptedException{
	   users = new ArrayList<>();
	   System.out.println("Users size before setup");
	   System.out.println(users.size());
	   if (context.getCacheFiles() != null && context.getCacheFiles().length > 0){
		   userFileUri = context.getCacheFiles()[0];
           usersFile = new File(userFileUri.toString());
	   }
	   try {
		   BufferedReader reader = new BufferedReader(new FileReader(usersFile));
           String line;
           while  ((line = reader.readLine()) != null){
               users.add(line);
           }
           reader.close();
       } catch (IOException x){
           System.err.format("IOException: %s%n", x);
       }
   }

   //key:customer values:[movie1:rating1,movie2:rating2...]
   //user,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating4
   //user,customer,sim,movie1:rating1,movie2:rating2
   public void reduce(Text key, Iterable<Text> values,Context context
                ) throws IOException, InterruptedException {
	   String customerId = key.toString();
	   ArrayList<String> valList = new ArrayList<>();
	   for(Text val : values){
		   valList.add(val.toString());
	   }	   
	   for(int i = 0; i < users.size(); i++){
		   String[] user = users.get(i).split(",");
		   String userId = user[0];
		   String[] predictList = user[1].split("\\+");
		   String[] otherList = user[2].split("\\+");
		   String result = "";
		   boolean ifRatedCommonMovie = false;
		   float sim = 0;
		   float sqr_sum = 0;
		   if(customerId.equals(userId)){
			   continue;
		   }else{
			   for(String val : valList){
				   String[] c_pair = val.toString().split(":");
				   String c_movieId = c_pair[0];
				   String c_rating = c_pair[1];
				   for(int j = 0; j < predictList.length; j++){
					   String[] predict_pair = predictList[j].split(":");
					   String predict_movie_id = predict_pair[0];
					   String predict_movie_rating = predict_pair[1];
					   if(predict_movie_id.equals(c_movieId)){
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
				   outValue.set(result);
				   context.write(null,outValue);
			   }
		   }
	   }
   }
}


public static class SimPartitioner extends Partitioner<Text, Text>{
	
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

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "calculate similarity");
    job.setJarByClass(CalSim.class);
    
    //MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,SimMapper.class);
    MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,SimMapper.class);
//    MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,SimMapper.class);
//    MultipleInputs.addInputPath(job,new Path(args[4]),TextInputFormat.class,SimMapper.class);
//    MultipleInputs.addInputPath(job,new Path(args[5]),TextInputFormat.class,SimMapper.class);
//    MultipleInputs.addInputPath(job,new Path(args[6]),TextInputFormat.class,SimMapper.class);
//    MultipleInputs.addInputPath(job,new Path(args[7]),TextInputFormat.class,SimMapper.class);
    
    job.setPartitionerClass(SimPartitioner.class);
    job.setNumReduceTasks(10);
    job.setReducerClass(SimReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.addCacheFile(new Path(args[0]).toUri());
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[8]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

