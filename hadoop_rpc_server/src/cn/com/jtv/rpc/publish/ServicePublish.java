package cn.com.jtv.rpc.publish;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import cn.com.jtv.rpc.proocal.LoginServiceProtocal;
import cn.com.jtv.rpc.server.LoginServiceImpl;

public class ServicePublish {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Builder builder = new RPC.Builder(conf);
		builder.setBindAddress("localhost")
		.setPort(10000)
		.setProtocol(LoginServiceProtocal.class)
		.setInstance(new LoginServiceImpl());
		
		Server server = builder.build();
		server.start();
	}

}
