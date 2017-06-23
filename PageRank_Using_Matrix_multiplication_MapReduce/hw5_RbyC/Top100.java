package hw5_RbyC;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Top100 {

}
// input format:R,x,y -tab- pagerank
class Top100Mapper extends Mapper<Object, Text, DoubleWritable, Text>{
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException{
		// x position of cell represents the id of URL
		// split and get fileds
		String input = value.toString();
		System.out.println(input);
		String in[] = input.split("\t");
		//System.out.println(in.length);

		String id = in[0].split(",")[1];
		String PR = in[1];
		double pr = Double.parseDouble(PR);

		//Emit the page rank and text respectively, so that the key comparator can compare the keys.
		ctx.write(new DoubleWritable(pr), new Text(id));



	}
}

class PagePartitioner extends Partitioner{

	@Override
	public int getPartition(Object arg0, Object arg1, int numRedTasks) {
		// TODO Auto-generated method stub
		return 0;
	}

}


// compare keys
class KeyComparator extends WritableComparator{

	public KeyComparator()
	{
		super(DoubleWritable.class,true);
	}

	public int compare(WritableComparable w1, WritableComparable w2)
	{
		DoubleWritable key1 = (DoubleWritable) w1;
		DoubleWritable key2 = (DoubleWritable) w2;
		int result  = Double.compare(key2.get(),key1.get());
		return result;
	}
}

// key is page rank, value is id
// emits first 100 inputs with id as key(value from map) and pagerank as 
//value(also a field PR is added to identify pagerank from its URL in nect iteratiion
class Top100Reducer extends Reducer<DoubleWritable, Text, Text, Text>{
	static int c = 100;
	public void reduce(DoubleWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException{

		for(Text t:values){
			if(c>0){ //A counter for top 100.
				ctx.write(t, new Text(key.toString()+"~"+"PR"));
				c--;
			}
		}
	}	

}

