package com.liumeng.udf;
import org.apache.hadoop.hive.ql.exec.UDF;

public class row_number extends UDF{

	private static int MAX_VALUE = 50;
	private static String last[] = new String[MAX_VALUE];
	private static int rowNumber = 1;
	
	public int evaluate (Object ...args){
		String now[] = new String[args.length];
		
		for(int i=0;i<args.length;i++)
			now[i]=args[i].toString();
		
		if(rowNumber == 1){
			for(int i=0;i<args.length;i++){
				last[i]=now[i];				
			}
		}
		
		for(int i=0;i<now.length;i++){
			if(!now[i].equals(last[i])){
				for(int j=0;j<now.length;j++){
					last[j]=now[j];
				}
				rowNumber=1;
				return rowNumber++;
			}
			
		}
		
		
		return rowNumber++;
	}
	
	
}
