package hw1;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class ve5_nosharing {
	public static int cnt;
	public static void main(String[] args) throws Exception {
		ve5_nosharing("/Users/anvitasurapaneni/Downloads/1912.csv");
	}


	static void ve5_nosharing(String str){

		System.out.println("VE5-non sharing- with out Fibonnaci");

		//PrintWriter writer = new PrintWriter ("file_version5.txt");

		String line;
		List<String> lines = new ArrayList<String>();

		try (
				InputStream fis = new FileInputStream(str);
				InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
				BufferedReader br = new BufferedReader(isr);
				) {
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		int n1 = lines.size() / 4;
		int n2 = lines.size() / 3;
		int n3 = lines.size() / 4 * 3 ;
		int n4 = lines.size() ;

		List<String> al1 = new ArrayList<String>(lines.subList(0, n1));
		List<String> al2 = new ArrayList<String>(lines.subList(n1, n2));
		List<String> al3 = new ArrayList<String>(lines.subList(n2, n3));
		List<String> al4 = new ArrayList<String>(lines.subList(n3, n4));

		long sumtime  = 0;
		long max = 0;
		long min = 100000;


		for(int i = 0; i < 10; i++){
			long time1 = System.currentTimeMillis();
			
			// dividing load equally to all threads
			WorkThread5 t11 = new WorkThread5(al1);
			WorkThread5 t22 = new WorkThread5(al2);
			WorkThread5 t33 = new WorkThread5(al3);
			WorkThread5 t44 = new WorkThread5(al4);
			// starting and running threasds
			Thread t1 = new Thread(t11);
			Thread t2 = new Thread(t22);
			Thread t3 = new Thread(t33);
			Thread t4 = new Thread(t44);


			t1.start();
			t2.start();
			t3.start();
			t4.start();

			try {
				t1.join();
				t2.join();
				t3.join();
				t4.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}




			HashMap<String,SumAndCount> r1 = t11.stationAndTmax;
			HashMap<String,SumAndCount> r2 = t22.stationAndTmax;
			HashMap<String,SumAndCount> r3 = t33.stationAndTmax;
			HashMap<String,SumAndCount> r4 = t44.stationAndTmax;

			//        
			//        System.out.println("r1 size:   "+r1.size());
			//        System.out.println("r2 size:   "+r2.size());
			//        System.out.println("r3 size:   "+r3.size());
			//        System.out.println("r4 size:   "+r4.size());


			// combining the results of all threads r1 - r4
			// r1    

			for(Entry<String,SumAndCount> m:r1.entrySet())

			{
				String key = m.getKey();
				SumAndCount sandc = m.getValue();
				float res;
				float cnt = sandc.count;
				float sum = sandc.sum;

				SumAndCount sandc2 = r2.get(key);
				SumAndCount sandc3 = r3.get(key);
				SumAndCount sandc4 = r4.get(key);

				if(sandc2 != null){
					cnt = cnt + sandc2.count;
					sum = sum + sandc2.sum;
					r2.remove(key);
				}

				if(sandc3 != null){
					cnt = cnt + sandc3.count;
					sum = sum + sandc3.sum;
					r3.remove(key);
				}

				if(sandc4 != null){
					cnt = cnt + sandc4.count;
					sum = sum + sandc4.sum;
					r4.remove(key);
				}



				res = sum/cnt;
				//	System.out.println(m.getKey()+"\t"+res);
				//	writer.println(m.getKey()+"\t"+res);

			} 

			// r2


			for(Entry<String,SumAndCount> m:r2.entrySet())

			{
				String key = m.getKey();
				SumAndCount sandc = m.getValue();
				float res;
				float cnt = sandc.count;
				float sum = sandc.sum;

				SumAndCount sandc1 = r1.get(key);
				SumAndCount sandc3 = r3.get(key);
				SumAndCount sandc4 = r4.get(key);

				if(sandc1 != null){
					cnt = cnt + sandc1.count;
					sum = sum + sandc1.sum;
					r1.remove(key);
				}

				if(sandc3 != null){
					cnt = cnt + sandc3.count;
					sum = sum + sandc3.sum;
					r3.remove(key);
				}

				if(sandc4 != null){
					cnt = cnt + sandc4.count;
					sum = sum + sandc4.sum;
					r4.remove(key);
				}



				res = sum/cnt;
				//System.out.println(m.getKey()+"\t"+res);
				//writer.println(m.getKey()+"\t"+res);

			} 

			//r3

			for(Entry<String,SumAndCount> m:r3.entrySet())

			{
				String key = m.getKey();
				SumAndCount sandc = m.getValue();
				float res;
				float cnt = sandc.count;
				float sum = sandc.sum;

				SumAndCount sandc2 = r2.get(key);
				SumAndCount sandc1 = r1.get(key);
				SumAndCount sandc4 = r4.get(key);

				if(sandc2 != null){
					cnt = cnt + sandc2.count;
					sum = sum + sandc2.sum;
					r2.remove(key);
				}

				if(sandc1 != null){
					cnt = cnt + sandc1.count;
					sum = sum + sandc1.sum;
					r1.remove(key);
				}

				if(sandc4 != null){
					cnt = cnt + sandc4.count;
					sum = sum + sandc4.sum;
					r4.remove(key);
				}



				res = sum/cnt;
				//System.out.println(m.getKey()+"\t"+res);
				//writer.println(m.getKey()+"\t"+res);

			} 
			// r4

			for(Entry<String,SumAndCount> m:r4.entrySet())

			{
				String key = m.getKey();
				SumAndCount sandc = m.getValue();
				float res;
				float cnt = sandc.count;
				float sum = sandc.sum;

				SumAndCount sandc2 = r2.get(key);
				SumAndCount sandc3 = r3.get(key);
				SumAndCount sandc1 = r1.get(key);

				if(sandc2 != null){
					cnt = cnt + sandc2.count;
					sum = sum + sandc2.sum;
					r2.remove(key);
				}

				if(sandc3 != null){
					cnt = cnt + sandc3.count;
					sum = sum + sandc3.sum;
					r3.remove(key);
				}

				if(sandc1 != null){
					cnt = cnt + sandc1.count;
					sum = sum + sandc1.sum;
					r1.remove(key);
				}



				res = sum/cnt;

				//System.out.println("bm"+m.getKey()+"\t"+res);
				//writer.println(m.getKey()+"\t"+res);

			} 

			long time2 = System.currentTimeMillis();

			long t = time2 - time1;
			//System.out.println(t);
			sumtime = sumtime + t;
			if(t > max){
				max = t;
			}
			if(t < min){
				min = t;
			}
			//writer.close();

		}

		System.out.println("max:  "+max);
		System.out.println("min:  "+min);
		System.out.println("avg:  "+sumtime/10);


	}


}

// work thread
class WorkThread5 extends Thread {

	List<String> weatherlines;
	HashMap<String,SumAndCount> stationAndTmax;

	WorkThread5(List<String> al1) {
		weatherlines = al1;
		// The accumilation data structure which is unique to each thread
					// SumAndCount is a user defined data structure which has attributes sum and count
					// sum stores the sum of the tmax values and count stores count of readings
		stationAndTmax = new HashMap<String,SumAndCount>();

	}



	@Override
	public void run() {

		for(String l: weatherlines){
			String attrs[] = l.split(",");
			String stationID = attrs[0];
			String date = attrs[1];
			String type = attrs[2];
			String val = attrs[3];
			float val1 = Float.parseFloat(val);
			if(type.equals("TMAX")){
				SumAndCount T = stationAndTmax.get(stationID);
				SumAndCount sc;
				if(stationAndTmax.get(stationID) == null){
					SumAndCount sc1 = new SumAndCount(val1, 1);
					stationAndTmax.put(stationID, sc1);
				}

				else{
					float s = stationAndTmax.get(stationID).sum;
					float c = stationAndTmax.get(stationID).count;
					s = s + val1;
					c= c + 1;
					stationAndTmax.get(stationID).setCount(c);
					stationAndTmax.get(stationID).setSum(s);

				}
			}

		}
	}
}
