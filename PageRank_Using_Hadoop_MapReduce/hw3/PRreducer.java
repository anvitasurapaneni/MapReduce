package HW3New;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import HW3New.pagerank.COUNTERS;

public class PRreducer 	extends Reducer<Text, Node, Text, Node> {
	private float dw;
	 long pagecount;
	public void setup(Context context){
	// Total weight(page rank) of all dangling Nodes from previous iteration
	 dw = context.getConfiguration().getFloat("dw", 0);
	 // count of total no of pages
	 pagecount = (long) context.getConfiguration().getFloat("PagesCount", (long)0);
	}
	
	// reduce function 
	// reduce recives Node object for Node M amd the page rank contributions for M's inlinks
	public void reduce(Text key, Iterable<Node> values,Context context) throws IOException, InterruptedException {

		// add value is the value of dangling Node's total weight divided by no of pages
		// this value is added to all page's page rank (equally distributing the page rank of dangling Nodes to all pages)
		float addval = 0;
		if(dw != 0){
			addval = (float)(((float)dw)/((float)pagecount));
		}

		// s is accumulation variable for storing sum of page ranks of all Nodes
		float s = (float) 0;
		Node M = new Node();
		
		
		// 1 is assigned as key to all dangling Nodes 
		// if it is a dangling Node, increment the counter COUNTERS.DanglingTotalPR  by the pade rank of the url
		
			if(key.toString().equals("1")){
			for(Node val: values){

				
			// multiply by a factor to avoid loss of precession.	
			float fpr =val.pr;
			long prtemp = (long)(fpr * 1000000000 * 10000000);
			
			//increment the counter COUNTERS.DanglingTotalPR  by the pade rank of the url
			context.getCounter(COUNTERS.DanglingTotalPR).increment(prtemp);
			// emit
			
			context.write(new Text(val.RootNodeName), val);
			}
			
		}
			
	// if key is not a dangling Node,
		else{
			for (Node val : values) {
				

				
			// condition to check if the key is a root Node(If it is a root Node, the RootNodeName and the key will be equal
				// because for a root Node, the key, RootNodeName attribute are set to the RootNodeName itself.
				// ie it is a Node
				if(val.RootNodeName.equals(key.toString())){
					// The Node object was found, recover graph structure
					M.links = val.links;
					M.RootNodeName = val.RootNodeName;
					M.IsDangling = val.IsDangling;
					M.pr = val.pr;
				}
				else{
					// page rank contribution from an inlink was found
					// add it to the running sum, s
					s = s+ val.pr;
				}
	
			}
		
		
		// taking alpha = 0, alpha/|V| + (1-alpha).s is eq to s
		M.pr = s;
		// addval is added to every Nodes pagerank
		M.pr = M.pr + addval;

		// emit
		context.write(key, M);
		}

	}

}