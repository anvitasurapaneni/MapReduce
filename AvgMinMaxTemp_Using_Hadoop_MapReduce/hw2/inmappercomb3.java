package hw2;



/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;

public class inmappercomb3 {

	// mapper class
	public static class InMapMapper
	extends Mapper<Object, Text, Text, tempattr> {

		HashMap<String, LinkedList<TypeAndtemperature3>> temps;

		// setup: Initialises Hashmap which stores stationID and a list of user defined datatype TypeAndtemperature3
		// TypeAndtemperature3: it has two attributes 1) type: weather it is TMAX or TMIN 2)temp: the temperature value
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			temps = new HashMap<String, LinkedList<TypeAndtemperature3>>();
		}


		//map: updates the hashmap
		// Iterates over each line from input file and processes it.
		// creates an object of TypeAndtemperature3 for each line, updates the hashmap based on key.
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


			String[] lines = value.toString().split(System.getProperty("line.separator"));

			for (String line: lines) {

				String attrs[] = line.split(",");
				String stationID = attrs[0];
				String date = attrs[1];
				String type = attrs[2];
				String val = attrs[3];
				float val1 = Float.parseFloat(val);
				TypeAndtemperature3 tt = new TypeAndtemperature3(type, val1);
				if((type.equals("TMAX") || type.equals("TMIN"))){

					if(temps.containsKey(stationID)){
						LinkedList<TypeAndtemperature3> ll =  new LinkedList<TypeAndtemperature3>();
						ll =	temps.get(stationID);

						ll.add(tt);
						temps.put(stationID, ll);
					}
					else if(!temps.containsKey(stationID)){
						LinkedList<TypeAndtemperature3> ll = new LinkedList<TypeAndtemperature3>();
						ll.add(tt);
						temps.put(stationID, ll);
					}

				}

			}


		}

		// cleanup: processes the hashmap, caliculates total Tmax, Total Tmin, Tmax Count, Tmin Count
		// and stores them in an object of type tempattr which is op of mapper and also input to reducer.
		//tempattr: It is a user defined data type which has 4 attributes:
		// tminSum  tminCnt tmaxSum  tmaxCnt
		
		public void cleanup(Context context) throws IOException, InterruptedException { 

			for(Map.Entry<String, LinkedList<TypeAndtemperature3>> m:temps.entrySet()){
				LinkedList<TypeAndtemperature3> v = m.getValue();

				float max = (float)0;
				float maxcnt = 0;
				float min = (float)0;
				float mincnt = 0;
				float avgmax = (float)0;
				float avgmin = (float)0;
				for (TypeAndtemperature3 val : v) {
					if(val.type.equals("TMAX")){

						max += val.temp;
						maxcnt++;
					}else
						if(val.type.equals("TMIN")){

							min += val.temp;
							mincnt++;
						}
				}
				tempattr ta = new tempattr(min, mincnt, max, maxcnt);

				context.write(new Text(m.getKey()), ta);

			}
		}
	}

	
	// reducer: Reduces data from different mappers and writes output to file
	// all the attributes of tempattr are accumilated here and the final average caliculation is done here
	// and op is written to file
	public static class ImMapReducer
	extends Reducer<Text,tempattr,Text,Text> {
		private Text result = new Text();
		private IntWritable one = new IntWritable(1);

		Text s;
		public void reduce(Text key, Iterable<tempattr> values,
				Context context
				) throws IOException, InterruptedException {

			float max = (float)0;
			float maxcnt = 0;
			float min = (float)0;
			float mincnt = 0;
			float avgmax = (float)0;
			float avgmin = (float)0;

			for (tempattr val : values) {

				max = max + val.tmaxSum;
				maxcnt = maxcnt + val.tmaxCnt;
				min = min + val.tminSum;
				mincnt = mincnt + val.tminCnt;
			}
			avgmax = max/ maxcnt;
			avgmin = min/ mincnt;
			String s = avgmin + ", " + avgmax;
			result.set(s);
			context.write(key, result);
		}
	}

// main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(inmappercomb3.class);
		job.setMapperClass(InMapMapper.class);
		//job.setCombinerClass(ImMapReducer.class);
		job.setReducerClass(ImMapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(tempattr.class);

		job.setNumReduceTasks(2);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}

//tempattr: It is a user defined data type which has 4 attributes:
// tminSum  tminCnt tmaxSum  tmaxCnt

class tempattr implements WritableComparable{


	public float tminSum;
	public float tminCnt;
	public float tmaxSum;
	public float tmaxCnt;

	public tempattr() {
		
	}

	public tempattr(Float tempmin, Float cntmin, Float tempmax, Float cntmax) {
		this.tminSum = tempmin;
		this.tminCnt = cntmin;
		this.tmaxSum = tempmax;
		this.tmaxCnt = cntmax;
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(tminSum);
		out.writeFloat(tminCnt);
		out.writeFloat(tmaxSum);
		out.writeFloat(tmaxCnt);
	}

	public void readFields(DataInput in) throws IOException {

		this.tminSum = in.readFloat();
		this.tminCnt = in.readFloat();
		this.tmaxSum = in.readFloat();
		this.tmaxCnt = in.readFloat();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}



// TypeAndtemperature3: it has 2 attributes
// it has two attributes 1) type: weather it is TMAX or TMIN 2)temp: the temperature value


class TypeAndtemperature3 implements WritableComparable {
	public String type;
	public float temp;

	public TypeAndtemperature3() {
	}

	public TypeAndtemperature3(String type, Float temp) {
		this.type = type;
		this.temp = temp;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(type);
		out.writeFloat(temp);
	}

	public void readFields(DataInput in) throws IOException {
		this.type = in.readUTF();
		this.temp = in.readFloat();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}




