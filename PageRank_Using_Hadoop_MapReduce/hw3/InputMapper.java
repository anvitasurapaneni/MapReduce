package HW3New;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InputMapper 

/* this input mapper converts the text input to Node
 Node stores Name: name of the url, RootNodeName: The Node's name who the the Nodes were outlinks of.
 If it is not an outlink, NodeName and Name are the same
 pr: which is the page rank of the page, links: Arraylist of outlinks for that rootNode If it is not a rootNode */

extends Mapper<Object, Text, Text, Node> {
	long pagecount;
	float ipr;
	ArrayList<String> alllinks;
	ArrayList<String> DanglingNodes;
	double totcnt = 0;
	//float ipr = (float)1/4;
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		 alllinks = new ArrayList<String>();
		 DanglingNodes= new ArrayList<String>();
		 pagecount = (long) context.getConfiguration().getFloat("PagesCount", (long)0);
		 ipr = (float)((float)1)/((float)pagecount);


	}
	
	
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {

		// the input from the previous job would give value of a line format"urlNamr	[nam1, name2,...]"
		// ie Node and its oulinks list
		// extract Nodename and its outlinks
		String[] inputs = value.toString().split("\t");
		String url = inputs[0];
		url = url.trim();
		String outlinkList = inputs[1]; //value.toString().trim();
		outlinkList = outlinkList.replace("[", "");
		outlinkList = outlinkList.replace("]", "");
		outlinkList = outlinkList.replace(" ", "");
		// outlinks list
		String[] outlinks = outlinkList.split(",");
		float outlinkssize = outlinks.length;
		Node n = new Node();
		n.RootNodeName = url;
		n.Name = url;
		n.pr = ipr;
		int oc =0;
		// if the outlinkList string does not contain , it means that there are no outlinks, ie dangling Node. 
		if(!outlinkList.contains(",")){
				n.IsDangling = true;
				ArrayList<String> empty = new ArrayList<String>();
				n.links = empty;
				context.write(new Text("1"), n);
		}
		else{
			// links store all out links
		ArrayList<String> links = new ArrayList<String>();
		Node p = new Node();
		// empty is an empty arraylist
		ArrayList<String> empty = new ArrayList<String>();
		p.RootNodeName = url;
		// compute contributions to send along outgoing links
		p.pr = (float)(ipr/outlinkssize);
	//	System.out.println(p.pr);
		p.links = empty;
		for(String link: outlinks){
			p.Name = link;
			link = link.trim();
			links.add(link);
			if(!link.equals("")){
				// sending contribution along outgoing links
			context.write(new Text(link), p);
			}
			totcnt = totcnt+ p.pr;
		}
		
	// pass along the graph structure
	n.links = links;

	context.write(new Text(n.RootNodeName), n);
	totcnt = totcnt+ n.pr;

	}
		
	}
	

	
	}