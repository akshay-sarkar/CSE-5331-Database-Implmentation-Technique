import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

public class AvgSalary {

	public static final String[] STATE_ARR = { "", "Alabama/AL",  "Alaska/AK", "","Arizona/AZ","Arkansas/AR","California/CA","", "Colorado/CO", "Connecticut/CT","Delaware/DE", "District of Columbia/DC", "Florida/FL", "Georgia/GA", "",	"Hawaii/HI", "Idaho/ID", "Illinois/IL", "Indiana/IN", "Iowa/IA", "Kansas/KS", "Kentucky/KY", "Louisiana/LA","Maine/ME",	"Maryland/MD","Massachusetts/MA","Michigan/MI","Minnesota/MN","Mississippi/MS","Missouri/MO","Montana/MT",	"Nebraska/NE", "Nevada/NV", "New Hampshire/NH", "New Jersey/NJ", "New Mexico/NM", "New York/NY","North Carolina/NC",	"North Dakota/ND", "Ohio/OH", "Oklahoma/OK", "Oregon/OR", "Pennsylvania/PA", "", "Rhode Island/RI", "South Carolina/SC","South Dakota/SD", "Tennessee/TN",  "Texas/TX", "Utah/UT", "Vermont/VT", "Virginia/VA","", "Washington/WA", "West Virginia/WV", "Wisconsin/WI","Wyoming/WY","","","","","","","","","","","","","","","","Puerto Rico/PR"};

	public static Text mapperKey = new Text();
	//mapper
	public static class AvgSalaryMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
			String line = value.toString();
			Log log = LogFactory.getLog(AvgSalaryMapper.class);

			try{
				//Clean the CSV Input
				line = line.replaceAll("[^A-Za-z0-9 ,]", "");

				String [] tokens  = line.split("\\s*,\\s*");
				String year = tokens[0].trim().substring(0,4); //first 4 digit
				String st_key =  tokens[5].trim();
				String gender =  tokens[69].trim();//male 1; female 2
				String sal = tokens[72].trim();
				if(sal != null && !sal.isEmpty() && (!sal.equals("0")) && st_key != null && !st_key.isEmpty()){
					double salary = Double.parseDouble(sal);
					int state = Integer.parseInt(st_key);
					log.info("Akshay Sarkar-> year "+year + " state"+state+" gender"+gender+" sal"+salary);
					//checking for empty salary BUT NOT Zero
					if(!gender.isEmpty() && ( gender.equals("1") || gender.equals("2")) && (salary>0 && salary<=999999)) {
						mapperKey.set(state+ "_" + new Text(year)  + "_" + new Text(gender));
						c.write(mapperKey, new DoubleWritable(salary));
					}
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
	public static class AvgSalaryReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		Log log = LogFactory.getLog(AvgSalaryMapper.class);
		@Override
		public void reduce(Text key, Iterable<DoubleWritable>values, Context c) throws IOException,InterruptedException{
			double count = 0.0;
                        double sum = 0.0;
			for(DoubleWritable val:values){
				count += 1;
				sum+=val.get();
			}
			double avg = sum/count;
			String [] plots = key.toString().split("_");
			String reducerkey;

			log.info("State "+STATE_ARR[Integer.parseInt(plots[0])]);
			if(plots[2].equals("1")){
				reducerkey = "For Year "+plots[1]+", in " + STATE_ARR[Integer.parseInt(plots[0])] + " the average salary of Male is ";
			}else{
				reducerkey = "For Year "+plots[1]+", in "+ STATE_ARR[Integer.parseInt(plots[0])] +" the average salary of Female is ";
			}
			c.write(new Text(reducerkey), new DoubleWritable(avg));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
                //conf.setLong("dfs.blocksize",64000000);
		Job j2 = Job.getInstance(conf);
		j2.setJobName("AvgSalarycount job");
		j2.setJarByClass(AvgSalary.class);

		//Mapper input and output
		j2.setMapOutputKeyClass(Text.class);
		j2.setMapOutputValueClass(DoubleWritable.class);

		//Reducer input and output
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(DoubleWritable.class);

		//file input and output of the whole program
		j2.setInputFormatClass(TextInputFormat.class);
		j2.setOutputFormatClass(TextOutputFormat.class);
		//Set the mapper class
		j2.setMapperClass(AvgSalaryMapper.class);
		//set the combiner class for custom combiner
		//j2.setCombinerClass(AvgSalaryReducer.class);

		//Set the reducer class
		j2.setReducerClass(AvgSalaryReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		//j2.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(j2, new Path(args[1]));
		FileInputFormat.addInputPath(j2, new Path(args[0]));

		// j2.waitForCompletion(true);
		int code = j2.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}
}
