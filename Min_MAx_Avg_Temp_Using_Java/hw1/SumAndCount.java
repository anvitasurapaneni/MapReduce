package hw1;

// User defined data structure to store the total sum of tmax values per station and no of
// t maxs per that station


class SumAndCount{
	float sum;
	float count;
	
	SumAndCount(float s, float c){
		sum = s;
		count = c;
	}
	
	void setSum(float s){
		this.sum = s;
	}
	
	void setCount(float c){
		this.count = c;
	}
	
// Fibonacci function for crating delay in executions
	public static int fibonacci(int n)  {
	    if(n == 0)
	        return 0;
	    else if(n == 1)
	      return 1;
	   else
	      return fibonacci(n - 1) + fibonacci(n - 2);
	}
}