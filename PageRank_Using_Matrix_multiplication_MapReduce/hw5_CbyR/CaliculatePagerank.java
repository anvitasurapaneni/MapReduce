package hw5_CbyR;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public class CaliculatePagerank{

}

// mapper recives input of text (x,y)-cell position and its value 
class CaliculatePagerankMapper
extends Mapper<Object, Text, Text, DoubleWritable> {


	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// separate input by tab :-  gives x,y and value
		// emit x,y as key and value as value
		String[] vals = value.toString().split("\t");
		context.write(new Text(vals[0]), new DoubleWritable(Double.parseDouble(vals[1])));
	}



}

// under reduce, we add all values belonging to one cell and caliculate pagerank for it
// emits all entries for R matrix(its position(x,y) and its pagerank value
class CaliculatePagerankReducer
extends Reducer<Text, DoubleWritable, NullWritable, Text> {
	long cnt;
	float  dangling;
	public void setup(Context context){
		Configuration conf = context.getConfiguration();
		cnt = conf.getLong("count", 1);
		// getting the dangling contribution value
		dangling = conf.getFloat("danglingScore", 1);
	}


	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = (double) 0;
		// acumilate values
		for(DoubleWritable val : values){
			sum = sum + val.get();
		}
		// caliculate pagerank and add dangling contribution  to each page
		sum = 0.15/cnt + 0.85*(sum + dangling);
		context.write(NullWritable.get(), new Text("R"+","+key+","+sum));

	}



}