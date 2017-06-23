package HW3New;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import HW3New.pagerank.COUNTERS;



public class PRmapper  extends Mapper<Text, Node, Text, Node> {
	//map processes the Node with id key
	//n or value stores key's current pagerank , its adjacency list, IsDangling says weather it is dangling Node r not,
	// it also stores its NodeName and its name
	public void map(Text key, Node value, Context context
			) throws IOException, InterruptedException {
// p
		Node n = new Node();
		n.RootNodeName = value.RootNodeName;
		n.pr = value.pr;
		n.Name = value.Name;
		n.IsDangling = value.IsDangling;
		n.links = value.links;
		
		// all dangling Nodes are sent with key 1 to be able to accumilate its pagerank values at reducer
		if(n.IsDangling){
	
			context.write(new Text("1"), value);
		
		}
		else{
		// pass along the graph structure
		context.write(new Text(n.RootNodeName), n);
		ArrayList<String> links = n.links;
		
		Node p = new Node();
		float linkssize = (float)links.size();
		ArrayList<String> empty = new ArrayList<String>();
		// compute contribution to send along outgoing links
		p.pr = (float)(n.pr/linkssize);
		
		
		for(String link: n.links){
			p.Name = link;
			context.write(new Text(link), p);
		}

	
		}
	}

}

