package cn.com.jtv.mr.test;

import java.io.FileOutputStream;
import java.util.Random;

public class Test1 {
	public static void main(String[] args) throws Exception {

		FileOutputStream fo = new FileOutputStream("d:/phoneData3.txt");
		int i = 0;
		while (true) {
			String phoneId = getPhoneId(new Random().nextInt(5));
			String upFlow = (int) (Math.random() * 10000) + "";
			String dwFlow = (int) (Math.random() * 1000) + "";

			byte[] b = (phoneId + " " + upFlow + " " + dwFlow + "\r\n").getBytes();
			fo.write(b);

			i++;
			System.out.println(i);
			if (i == 10000000) {
				break;
			}

		}

		fo.close();

	}

	public static String getPhoneId(int index) {
		String[] phoneId = { "14563547822", "156985224", "8655467", "8766347", "8369182" };
		return phoneId[index];
	}
}
