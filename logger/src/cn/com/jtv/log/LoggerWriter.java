package cn.com.jtv.log;

import org.apache.log4j.Logger;

public class LoggerWriter {
	public static void main(String[] args) throws Exception {
		
		
		while(true){
			Logger log = Logger.getLogger("logRollingFile");
			log.info("11111111111111111111111111");
			Thread.sleep(100);
		}
		
	}
	
}
