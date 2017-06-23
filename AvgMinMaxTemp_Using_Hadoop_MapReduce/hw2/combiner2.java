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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class combiner2 {

	
	//mapper
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, TypeAndtempAttrs> {

    			//map: Iterates over each line from input file and processes it.
    			//creates object of type TypeAndtempAttrs which stores Type ie TMAX(sum of all Tmax s, if it is only one, max count attribute will be 1)
    	// 	TMIN(Similar to TMAX)  maxcnt(Total no of Tmax values acvumilated) mincnt(Simiar to tmaxcnt)
    			//writes it with the station ID as the key and the object as its value this will be input to combiner
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

        	 
            String[] lines = value.toString().split(System.getProperty("line.separator"));
            
            for (String line: lines) {
          	  
          		String attrs[] = line.split(",");
      			String stationID = attrs[0];
      			String date = attrs[1];
      			String type = attrs[2];
      			String val = attrs[3];
      			float val1 = Float.parseFloat(val);
          	  
      			TypeAndtempAttrs tt;
      			Text stID = new Text(stationID);
           // update only Tmax or Tmin values based on the type
            	 if(type.equals("TMIN")){
            		 tt = new TypeAndtempAttrs(type, 0, 0, val1, 1);
            		 context.write(stID, tt);
            	 }
            	 if(type.equals("TMAX")){
            		 tt = new TypeAndtempAttrs(type, val1, 1, 0, 0);
            		 
            		 context.write(stID, tt);
            	 }
                
             
            }


        }

    }
    
   // combiner iterates over the vaues of mapper per key, caliculates local Tmax(Tmax sum), tmin(Tmin sum), tmaxcnt, tmincnt
    // the type is replaced by "ALL" to representt it has both Tmax and Tmin values
    public static class Combiner extends Reducer<Text,TypeAndtempAttrs, Text,TypeAndtempAttrs> 
    {


        float avgmax = 0;
        float avgmin = 0;

        public void reduce(Text key, Iterable<TypeAndtempAttrs> values,
                           Context context
        ) throws IOException, InterruptedException {
            float max = 0;
            float maxcnt = 0;
            float min = 0;
            float mincnt = 0;
            for (TypeAndtempAttrs val : values) {
            	if(val.type.equals("TMAX")){
            		//System.out.println(val.tmax);
            		//if(val.tmin != 100000){
            			max += val.tmax;
            			maxcnt += val.maxcnt;
            		//}
            		
            	}
            	if(val.type.equals("TMIN")){
            		//System.out.println(val.tmin);
            		//if(val.tmax != 100000){
            			min += val.tmin;
            			mincnt += val.mincnt;
            		//}
            		
            	}
            	

            	//if()
            	
            }
            //System.out.println(min+" "+mincnt+" "+max+" "+maxcnt);
            TypeAndtempAttrs tt1 = new TypeAndtempAttrs("ALL", max, maxcnt, min, mincnt);
           
            context.write(key, tt1);

        }
    
    }
    
    
// reducer recombines the results for teh same key and computes the Tmin avg and Tmax average
    public static class IntSumReducer
            extends Reducer<Text,TypeAndtempAttrs,Text,Text> {
    	public void reduce(Text key, Iterable<TypeAndtempAttrs> values,
                Context context
) throws IOException, InterruptedException {
    		float tmaxavg = 0;
    		float tminavg = 0;
    		
    		float max = 0;
            float maxcnt = 0;
            float min = 0;
            float mincnt = 0;
            
    		 for (TypeAndtempAttrs val : values) {
             	
             if(val.type.equals("ALL")){
            	 max = max + val.tmax;
            	 min = min + val.tmin;
            	 maxcnt = maxcnt + val.maxcnt;
            	 mincnt = mincnt + val.mincnt;
             }
        }
    		 
    		 tmaxavg = max/maxcnt;
    		 tminavg = min/mincnt;
    		String s = tminavg + ", " + tmaxavg;
    		
    		  context.write(key,new Text(s));
    	
    	}
    	
    	
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(combiner2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(2);
        job.setCombinerClass(Combiner.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TypeAndtempAttrs.class);

       
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//TypeAndtempAttrs: It is a user defined data type which has 4 attributes:
//type tmax: Sum of tmax values  tmin:sum of tmin values  tmaxcnt: count of Tmax records  tmincnt: count of Tmin records

class TypeAndtempAttrs implements WritableComparable {
    public String type;
    public float tmax;
    public float tmin;
    public float maxcnt;
    public float mincnt;

    public TypeAndtempAttrs() {
    }
    
    public TypeAndtempAttrs(String type, float tmax, float maxcnt, float tmin, float mincnt) {
        this.type = type;
        
        this.tmax = tmax;
        this.maxcnt = maxcnt;
        
        this.tmin = tmin;
        this.mincnt = mincnt;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(type);
        out.writeFloat(tmax);
        out.writeFloat(maxcnt);
        out.writeFloat(tmin);
        
        out.writeFloat(mincnt);
    }

    public void readFields(DataInput in) throws IOException {
        this.type = in.readUTF();
        this.tmax = in.readFloat();
        this.maxcnt = in.readFloat();
        this.tmin = in.readFloat();
        this.mincnt = in.readFloat();
    }

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

   

 
}






//tempattr2: It is a user defined data type which has 4 attributes:
//tminSum  tminCnt tmaxSum  tmaxCnt

class tempattr2 implements WritableComparable{


	public float tminSum;
	public float tminCnt;
	public float tmaxSum;
	public float tmaxCnt;

	public tempattr2() {
	}

	public tempattr2(Float tempmin, Float cntmin, Float tempmax, Float cntmax) {
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

