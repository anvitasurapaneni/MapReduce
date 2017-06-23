package HW3New;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;

// partitioner to ensure all records are sent to one file only.
class SortPartitioner extends Partitioner{

	@Override
	public int getPartition(Object arg0, Object arg1, int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}

// sorts the entries and emits only top 100 pages with highest pageranks.
public class SortReducer extends Reducer<Text,Node,Text,Text>{
	HashMap<String, Float> temps;
	private static final String DecimalFormat = "######.##########";
	// initilaise hashmap
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

	 temps = new HashMap<String, Float>();
	}
	//  collect URL and pagerank of all Nodes in hashmap
	public void reduce(Text key, Iterable<Node> values,
			Context context
			) throws IOException, InterruptedException{
		String url = key.toString();
		float pr = (float)0;
		for (Node val : values) {
			pr = (float)val.pr;
			temps.put(url, pr);
		}
		
	}
	
	// sort hashmap and emit top 100 pages
	public void cleanup(Context context) throws IOException, InterruptedException { 
		 java.text.DecimalFormat decimalFormat = new java.text.DecimalFormat(DecimalFormat);
		 // sort hashmap
		HashMap<String, Float> temps2 = sortByValues(temps);
		int count = 0;
		for(Map.Entry<String, Float> m:temps2.entrySet()){
			//emit top 100 pages
			if(count < 100){
				count++;
			String str =	decimalFormat.format(m.getValue()).toString();
				context.write(new Text(m.getKey()), new Text(str));
		 	  	   	}
			
		}
	}
	
	// sort hashmap by value function definition
    
    private static HashMap<String, Float> sortByValues(Map<String, Float> aMap) {


        Set<Entry<String,Float>> mapEntries = aMap.entrySet();



       List<Entry<String,Float>> aList = new LinkedList<Entry<String,Float>>(mapEntries);



 Collections.sort(aList, new Comparator<Entry<String,Float>>() {



 public int compare(Entry<String, Float> ele1,



                    Entry<String, Float> ele2) {



                return ele2.getValue().compareTo(ele1.getValue());



            }

 });



Map<String,Float> aMap2 = new LinkedHashMap<String, Float>();



 for(Entry<String,Float> entry: aList) {



     aMap2.put(entry.getKey(), entry.getValue());

}

return (HashMap<String, Float>) aMap2;

} 

	
}
	
