import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class CountAge {

	public static Text mapperKey = new Text();

	//mapper
	public static class CountAgeMapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
			String line = value.toString();
			Log log = LogFactory.getLog(CountAgeMapper.class);
			
			try{
				//Clean the CSV Input
				line = line.replaceAll("[^A-Za-z0-9 ,]", "");
				
				//Logging the Info
				//log.info("Akshay Sarkar -> line "+line);
				
				String [] tokens  = line.split("\\s*,\\s*");
				String year = tokens[0].trim().substring(0,4); //first 4 digit
				String strAge =  tokens[8].trim();

				//Checking for the age
				if(strAge != null && !strAge.isEmpty()){
					int age = Integer.parseInt(strAge);
					log.info("Akshay Sarkar-> year "+year + " age"+age);

					String ageGroup = "Above 100";
					if(0<=age && age<=9){
						ageGroup = "0-9";
					}else if(10<=age && age<=19){
						ageGroup = "10-19";
					}else if(20<=age && age<=29){
						ageGroup = "20-29";
					}else if(30<=age && age<=39){
						ageGroup = "30-39";
					}else if(40<=age && age<=49){
						ageGroup = "40-49";
					}else if(50<=age && age<=59){
						ageGroup = "50-59";
					}else if(60<=age && age<=69){
						ageGroup = "60-69";
					}else if(70<=age && age<=79){
						ageGroup = "70-79";
					}else if(80<=age && age<=89){
						ageGroup = "80-89";
					}else if(90<=age && age<=99){
						ageGroup = "90-99";
					}

					mapperKey.set(new Text(year) + "_" + new Text(ageGroup));
					c.write(mapperKey, new Text("1"));
				}else{
					log.info("Ignoring since age is empty ..");	
				}
			}catch(ArrayIndexOutOfBoundsException e){
				log.info("ArrayIndexOutOfBoundsException Handling..");
			}catch(NumberFormatException e){
				log.info("NumberFormatException Handling..");
			}catch(Exception e){
				log.info("Ignoring..");
			}
		}
	}

	//reducer
	public static class CountAgeReducer extends Reducer<Text,Text,Text,Text>{

		Log log = LogFactory.getLog(CountAgeMapper.class);
		@Override
		public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{			
			int count = 0;
			for(Text val:values){
				count += 1;
			}
			String [] plots = key.toString().split("_");
			c.write(new Text("For Year "+plots[0]+", the count for Age Group "+plots[1]+" is "), new Text(""+count));	

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
			Configuration conf = new Configuration();
			Job j2 = Job.getInstance(conf);
			j2.setJobName("CountAge job");
			j2.setJarByClass(CountAge.class);
			
			//Mapper input and output
			j2.setMapOutputKeyClass(Text.class);
			j2.setMapOutputValueClass(Text.class);
			
			//Reducer input and output
			j2.setOutputKeyClass(Text.class);
			j2.setOutputValueClass(Text.class);

			//file input and output of the whole program
			j2.setInputFormatClass(TextInputFormat.class);
			j2.setOutputFormatClass(TextOutputFormat.class);
			//Set the mapper class
			j2.setMapperClass(CountAgeMapper.class);
			//set the combiner class for custom combiner
			//j2.setCombinerClass(WordReducer.class);
			
			//Set the reducer class
			j2.setReducerClass(CountAgeReducer.class);
			
			//set the number of reducer if it is zero means there is no reducer
			//j2.setNumReduceTasks(0);
			FileOutputFormat.setOutputPath(j2, new Path(args[1]));
			FileInputFormat.addInputPath(j2, new Path(args[0]));


			// j2.waitForCompletion(true);
			int code = j2.waitForCompletion(true) ? 0 : 1;
    		System.exit(code);
		}
}