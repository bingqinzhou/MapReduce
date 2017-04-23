package finalproject_try;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class Estimation {
	
	//user:movie,predicted_rating
	public static class ValMapper1 extends Mapper<Object, Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
				   String[] line=value.toString().split(","); 
				   String[] pair = line[0].split(":");
				   String user = pair[0];
				   String movie = pair[1];
				   String predicted_rating = line[1];
				   outkey.set(user+":"+movie);
				   outvalue.set("P"+predicted_rating);
				   context.write(outkey, outvalue);
		}
	}
	
	//user,movie1:rating1+movie2:rating2,movie3:rating3+movie4:rating4
    public static class ValMapper2 extends Mapper<Object, Text, Text, Text>{
    	Text outkey = new Text();
		Text outvalue = new Text();
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
				   String[] line=value.toString().split(","); 
				   String user = line[0];
				   System.out.println("ValMapper2 is called here !!!");
				   System.out.println(user);
				   String[] predictArr = line[1].split("\\+");
				   for(int i = 0; i < predictArr.length; i++){
					   String[] pair = predictArr[i].split(":");
					   String movie = pair[0];
					   String rating = pair[1];
					   outkey.set(user+":"+movie);
					   outvalue.set("A"+rating);
					   context.write(outkey, outvalue);
				   }
		}
	}
    
    public static class ValReducer extends Reducer<Text,Text,Text,Text> {
    	
    	//user:movie,[predicted_rating,actual_rating]
    	//key: number_of_user  value:sum_of_error
    	
    	Text outvalue = new Text();
    	
    	int num_of_predict = 0;
    	float sum_of_error = 0;
    	
    	public void reduce(Text key, Iterable<Text> values,Context context
    	                ) throws IOException, InterruptedException {
    		
    		float predict_rating = 0;
    		float actual_rating = 0;
    		
    		for(Text val : values){
    			String tmp = val.toString();
    			if(tmp.charAt(0) == 'P'){
    				predict_rating = Float.parseFloat(tmp.substring(1,tmp.length()));
    			}else{
    				actual_rating = Float.parseFloat(tmp.substring(1,tmp.length()));
    			}
    		}
    		float diff = Math.abs(predict_rating-actual_rating);
    		float error = diff/actual_rating;
    		num_of_predict++;
    		sum_of_error = sum_of_error+error;
    	}
    	
    	public void cleanup (Context context) throws IOException,InterruptedException {
    		outvalue.set(String.valueOf(num_of_predict)+","+String.valueOf(sum_of_error));
    		context.write(null, outvalue);
    	}
    }
    
    public static class ValPartitioner extends HashPartitioner<Text, Text>{
    	
    	@Override
        public int getPartition(Text key, Text result, int numReduceTasks) {
    		int num = key.toString().hashCode();
    		return (num&Integer.MAX_VALUE)%numReduceTasks;
        }  
    	
    }
    
    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "estimation");
	    job.setJarByClass(Estimation.class);
	    MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,ValMapper1.class);
	    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,ValMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,ValMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[3]),TextInputFormat.class,ValMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[4]),TextInputFormat.class,ValMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[5]),TextInputFormat.class,ValMapper2.class);
	    
	    job.setPartitionerClass(ValPartitioner.class);
	    job.setNumReduceTasks(5);
	    job.setReducerClass(ValReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[6]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

