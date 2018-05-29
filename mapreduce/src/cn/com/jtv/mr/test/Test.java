package cn.com.jtv.mr.test;

import java.io.FileOutputStream;
import java.util.Random;

public class Test {
	public static void main(String[] args) throws Exception {
		
		
		FileOutputStream fo = new FileOutputStream("d:/mv2.txt");
		int i = 0;
		while(true){
			String uid = new Random().nextInt(20)+"";
			String mid = (int)(Math.random()*10000)+"";
			String rate = new Random().nextInt(6)+"";
			
			byte[] b = (uid + "," + mid + "," + rate + "\r\n").getBytes();
			fo.write(b);
			
			i++;
			System.out.println(i);
			if(i == 1000){
				break;
			}
			
		}
		
		fo.close();
		
		
	}
	
	public static String getPhoneId(int index){
		String[] phoneId = {"13574456525","14875635489","16752148563","18688853654","18888888888"};
		return phoneId[index];
	}

}
