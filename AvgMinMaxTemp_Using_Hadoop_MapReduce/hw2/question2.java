package hw2;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.io.DataInput;
import java.io.DataOutput;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class question2 {
	//main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(question2.class);

		job.setMapperClass(SortMapper1.class);
		job.setPartitionerClass(SortPartitioner1.class);
		job.setReducerClass(SortReducer1.class);
		job.setMapOutputKeyClass(StIdAndYear.class);
		job.setMapOutputValueClass(TypeAndtemperature.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}



// mapper
class SortMapper1 extends Mapper<Object, Text, StIdAndYear, TypeAndtemperature> {

	private Text stID = new Text();

	//map: processes all lines of all input files in the given folder
	// writes op with key as an object of StIdAndYear class which has properties Station Id and Year
	// value will be object of StIdAndYear class with attributes type ie TMAX or TMIN followed by its temperature
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] lines = value.toString().split(System.getProperty("line.separator"));

		for(String line: lines) {
			
			String attrs[] = line.split(",");
			String stationID = attrs[0];
			String date = attrs[1];
			String year = date.substring(0, Math.min(date.length(), 4));
			String type = attrs[2];
			String val = attrs[3];
			Float val1 = Float.parseFloat(val);
			stID.set(stationID);
			if(type.equals("TMAX") || type.equals("TMIN")){
				StIdAndYear wl = new StIdAndYear(stationID, Integer.parseInt(year));
				TypeAndtemperature tt = new TypeAndtemperature(type, val1);
					context.write(wl, tt);

			}
		}
	}    
}


// partitions the input and sends to 2 reducers 
// the comparator of class StIdAndYear makes sure the data goes in sorted order to reducer i.e,
// first sorted by stationId and if the station Id is equal, sort by Year.

class SortPartitioner1 extends Partitioner<StIdAndYear, TypeAndtemperature> {
	@Override
	public int getPartition(StIdAndYear key, TypeAndtemperature value, int nrt) {
		char c = key.StId.charAt(0);
		if (c >= 'A' && c <= 'H') {
			return 0 % nrt;
		}
		if (c >= 'I' && c <= 'P') {
			return 1 % nrt;
		}
		else {
			return 2 % nrt;
		}
	}
}

// reducer
 class SortReducer1 extends Reducer<StIdAndYear,TypeAndtemperature,Text,Text> {

	HashMap<String, String> hm;
// setup: Initializes Hashmap.
	public void setup(Context context) {
		hm = new HashMap<String, String>();
	}
// reduce: extracts the station ID, caliculates averages for all years associated with the station Id
	// update the hashmap with the key as station ID,
	//value as a HashMap which has year, Tmin, Tmax sorted in order of year
	public void reduce(StIdAndYear key, Iterable<TypeAndtemperature> values, Context context)
			throws IOException, InterruptedException {

		Float max = (float)0;
		int maxcnt = 0;
		Float min = (float)0;
		int mincnt = 0;
		Float avgmax = (float)0;
		Float avgmin = (float)0;
		for (TypeAndtemperature val : values) {
			if(val.type.equals("TMAX")){
				max += val.temp;
				maxcnt++;
			}
			else if(val.type.equals("TMIN")){
				min += val.temp;
				mincnt++;
			}

		}

		avgmax = max/maxcnt;
		avgmin = min/mincnt;

		String key1 = key.StId;
		String val1 = "(" + key.year+ " , " + avgmin + ", " + avgmax + ")";

		if(hm.containsKey(key1)){
			String s1 = hm.get(key1);
			s1 = s1 + ", " + val1;
			hm.put(key1, s1);
		}

		else{
			hm.put(key1, val1);
		}

	}
// puts all the year and Tmin, Tmax in the same line for a given stationId in the sorted order of years.
	// write output with the key as station ID,
		//value as a string which has year, Tmin, Tmax sorted in order of year
	public void cleanup(Context context) throws IOException, InterruptedException { 

		for (Entry<String, String> m:hm.entrySet()) {
			context.write(new Text(m.getKey()), new Text("["+ m.getValue() + "]"));
		}
	}
}


//TypeAndtemperature: it has 2 attributes
//it has two attributes 1) type: weather it is TMAX or TMIN 2)temp: the temperature value

class TypeAndtemperature implements WritableComparable {
	public String type;
	public float temp;

	public TypeAndtemperature() {
	}

	public TypeAndtemperature(String type, Float temp) {
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


//StIdAndYear: it has 2 attributes
//it has two attributes 1) StId: StationId 2)year: year of reading temperature

class StIdAndYear implements WritableComparable {
	public String StId;
	public int year;

	public StIdAndYear() {
	}

	public StIdAndYear(String stid, int year) {
		this.StId   = stid;
		this.year = year;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(StId);
		out.writeInt(year);
	}

	public void readFields(DataInput in) throws IOException {
		this.StId   = in.readUTF();
		this.year = in.readInt();
	}

	// comapreTo : sorts based on station ID, If they are equal, sorts based on year
	public int compareTo(Object other) {
		return this.compareTo((StIdAndYear) other);
	}

	public int compareTo(StIdAndYear other) {
		int dl = this.year - other.year;
		int dw = this.StId.compareTo(other.StId);

		if (dw == 0) {
			return dl;
		}
		else {
			return dw;
		}
	}

	public int hashCode() {
		return year + StId.hashCode();
	}
}
