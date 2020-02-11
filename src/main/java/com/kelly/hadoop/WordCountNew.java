import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountNew {
	
	// Below is the Mapper Class Definition
	public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable>
	{   
		// key            value
		// 11111L , hadoop is bigdata tool----> hadoop	1
		//                                      is	1
		//                                      bigdata	1
		//                                      tool 1
		// Object ,     Text       -----------> Text   IntWritable
		//Local Variable Declaration
		private final static IntWritable one = new IntWritable(1);// to hold the VALUE data
		private Text word = new Text();// to hold KEY Data
		
		// Below method we have to override to write mapper business logic
		protected void map(Object key , Text value , Context context) throws IOException, InterruptedException
		{
			// INPUT DATA = 1111L , hadoop is bigdata tool
			StringTokenizer itr = new StringTokenizer(value.toString());
			// NULL
			// hadoop
			// is
			// bigdata
			// tool
			//Looping
			while(itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word,one);// hadoop	1
				                        // is	1
				                        // Text  IntWritable
			}
			
		}// End of map method
		
		
		
	}// End Of Mapper Class Definition
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		// hadoop    1,1,1,1 ------------> hadoop	4
		// Text      IntWritable---------> Text    IntWritable
		//Local Variable Declaration
		private IntWritable result = new IntWritable();
		//Below method we must have to override to provide reducer business logic
		protected void reduce(Text key , Iterable<IntWritable> values , Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable val:values)// 1,1,1,1
			{
				sum += val.get(); // sum = sum + val.get();
			}
			result.set(sum);// we are writing "Int" kind of sum --> IntWritable kind of result variable
			context.write(key,result);
		}// End of reduce method
		
		
		
	} // End of Reducer Class Definition
	/**
	 * @param args
	 * @throws IOException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		System.out.println(" ");
		System.out.println("**** In Driver Class Of Map Reducer Program ****");
		System.out.println(" ");
		
		// Configuration Details w.r.to JOB , JAR etc
		Configuration conf = new Configuration();// To refer configurations of hadoop
        Job job = new Job(conf,"WORDCOUNTJOB-107");	//Job Object creation	
        job.setJarByClass(WordCountNew.class);// Driver Class Name we have to provide here
        
        // Mapper , Combiner & Reducer Class Name Details alone
        job.setMapperClass(TokenizerMapper1.class);// This is the Mapper Class Name
        job.setCombinerClass(IntSumReducer.class);// To Increse the Performance of processing
        job.setReducerClass(IntSumReducer.class);// This is the Reducer Class Name
        
        // Final Output KEY , VALUE Data type details
        job.setOutputKeyClass(Text.class); // hadoop
        job.setOutputValueClass(IntWritable.class);// 4
        
        //Input & Output Path Details for exdecution
        FileInputFormat.addInputPath(job, new Path(args[0]));// Input Path - args[0]
        FileOutputFormat.setOutputPath(job, new Path(args[1]));// Output Path - args[1]
        
        // System Exit Process from phase to phase
        System.exit(job.waitForCompletion(true)?0:1);
	}

}







