package HW3New;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*Node stores Name: name of the url, RootNodeName: The node's name who the the nodes were outlinks of.
If it is not an outlink, NodeName and Name are the same
pr: which is the page rank of the page, links: Arraylist of outlinks for that rootnode If it is not a rootnode 
IsDangling says weather the node is dangling or not*/

	public class Node1 implements Writable {

		String RootNodeName;
		public  String Name;
		public float pr;
		public ArrayList<String> links;
		public  boolean IsDangling;

		public Node1() {
			this.RootNodeName = "";
			this.Name = "";
			this.pr = (float)0.0;
			this.links = new ArrayList<String>();
			IsDangling = false;
		}

		public Node1(String NodeName, float pr, ArrayList<String> links) {
			
			this.RootNodeName = NodeName;
			this.pr = pr;
			this.links = links;
		}

		public void write(DataOutput out) throws IOException {

			out.writeUTF(RootNodeName);
			
			out.writeUTF(Name);
			
			out.writeFloat(pr);
			
			out.writeInt(links.size());
			
			for (String dst : links) {
				out.writeUTF(dst);
			}
			out.writeBoolean(IsDangling);
		}

		public void addToList(String str){
			links.add(str);
		}
		
		public void readFields(DataInput in) throws IOException {

			this.RootNodeName = in.readUTF();
			
			this.Name = in.readUTF();
			
			this.pr = in.readFloat();
			
			int nn = in.readInt();
			links = new ArrayList<String>(nn);
			for (int ii = 0; ii < nn; ++ii) {
				links.add(in.readUTF());
			}
			
			this.IsDangling = in.readBoolean();
		}
		
		public String toString(){
			return " "+pr;
		}

		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}


	}
	
	

