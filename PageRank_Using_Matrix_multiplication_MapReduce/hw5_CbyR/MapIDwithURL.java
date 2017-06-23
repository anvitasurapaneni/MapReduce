package hw5_CbyR;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapIDwithURL {

}

// input format: ID -tab- Pagerank/ Url full name -delimiter(~)- Type(weather it is a pagerank or Full name)
class GetURLMapper extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException{

		String input = value.toString();
		String in[] = input.split("\t");
		String id = in[0];
		String PRorURL = in[1];
		// emit id as key and pagerank or name as value
		ctx.write(new Text(id), new Text(PRorURL));


	}
}

// under reducer, all entries with same id are sent to the same job
class GetURLReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	static int c = 100;
	TreeMap<String, Double> output;
	public void setup(Context c){
		output = new TreeMap<String, Double>();
	}

	public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException{
		Double pr = null;
		String URL = null;

		// for the vlaues of same key,identify if it is a pagerank value or a Full name
		// collect pr and name for the same ID
		for(Text t:values){
			String ts[] = t.toString().split("~");
			if(ts[1].equals("URL")){
				URL = ts[0];

			}

			else{
				pr = Double.parseDouble(ts[0]);
			}
		}
		// add pagerank and URL name to Treemap
		if(pr != null && URL != null){
			output.put(URL, pr);
		}
	}	

	// in clean up emit all URL name and pagerank values in descending order of pr and emit
	// this is the final output
	public void cleanup(Context c) throws IOException, InterruptedException{

		Map<String, Double> n = (TreeMap<java.lang.String, java.lang.Double>) sortByValues(output);
		for(Map.Entry<String, Double> e : n.entrySet()){
			c.write(new Text(e.getKey()), new DoubleWritable(e.getValue()));
		}
	}


	public static <String, Double extends Comparable<Double>> Map<String, Double> sortByValues(final Map<String, Double> map) {
		Comparator<String> valueComparator =  new Comparator<String>() {
			public int compare(String k1, String k2) {
				int compare = map.get(k2).compareTo(map.get(k1));
				if (compare == 0) return 1;
				else return compare;
			}
		};
		Map<String, Double> sortedByValues = new TreeMap<String, Double>(valueComparator);
		sortedByValues.putAll(map);
		return sortedByValues;
	}
}