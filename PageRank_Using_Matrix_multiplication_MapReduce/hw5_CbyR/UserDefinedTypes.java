package hw5_CbyR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserDefinedTypes{

}



// User defined data type which is writable and comparable:
//contains attributes: 
// type :- The type of the URL "N" represents that the URL is the node, "OL" represents that the URL is an outlink to a node
// pos :- tells the position of the URL, if it is N, it stores the unique id assigned to the URL, this will be its position in the graph
//		  if it is OL, it stores the position of its parent i.e. its corresponding N's position.
// OLsize :- It is the size of out links for its parent(Used to caliculate pagerank distribution)
//			 for N it is 0, for OL it is outlink size of its N
class UrlTypePosPRcount implements WritableComparable<UrlTypePosPRcount> {


	public String type;
	public long pos;
	public double OLsize;


	public UrlTypePosPRcount() {
	}

	public UrlTypePosPRcount(String t, long p, double PRcontribution) {
		this.type = t;
		this.pos = p;
		this.OLsize = PRcontribution;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(type);
		out.writeLong(pos);
		out.writeDouble(OLsize);
	}

	public void readFields(DataInput in) throws IOException {
		this.type = in.readUTF();
		this.pos = in.readLong();
		this.OLsize = in.readDouble();
	}

	@Override
	public int compareTo(UrlTypePosPRcount o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.type + "," + this.pos + "," + this.OLsize;
	}

}



//User defined data type which is writable and comparable:
//contains attributes: 
//type :- The type of the matrix M- represents matrix/ sparse matrix, R represents pagerqnk per URL, D represents dangling nodes
//x:- represents the x position
// y:- represents the y position
// PRcontribution:- page rank contribution of the cell has value i/ out link size


class Matrix_XY_PRcontribution implements WritableComparable{

	public String type;
	public long x;
	public long y;
	public double PRcontribution;


	public Matrix_XY_PRcontribution() {
	}

	public Matrix_XY_PRcontribution(String type, long x, long y, double PRcontribution) {
		this.type = type;
		this.x = x;
		this.y = y;
		this.PRcontribution = PRcontribution;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(type);
		out.writeLong(x);
		out.writeLong(y);
		out.writeDouble(PRcontribution);
	}

	public void readFields(DataInput in) throws IOException {
		this.type =in.readUTF();
		this.x = in.readLong();
		this.y = in.readLong();
		this.PRcontribution = in.readDouble();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.type + "," + this.x + "," + this.y + "," + this.PRcontribution;
	}




}


//User defined data type which is writable and comparable:
//contains attributes: 
//type :- The type of the matrix M- represents matrix/ sparse matrix, R represents pagerqnk per URL, D represents dangling nodes
//index is the index of cell
//value:- page rank cross product value or pagerank value

class MatrixTypeIndexVal implements WritableComparable {

	public String type;
	public long index;
	public double value;


	public MatrixTypeIndexVal() {
	}

	public MatrixTypeIndexVal(String matrix, long index, Double value) {
		this.type = matrix;
		this.index = index;
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(type);
		out.writeLong(index);
		out.writeDouble(value);
	}

	public void readFields(DataInput in) throws IOException {
		this.type =in.readUTF();
		this.index = in.readLong();
		this.value = in.readDouble();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.type + "," + this.index + "," + this.value;
	}

}

