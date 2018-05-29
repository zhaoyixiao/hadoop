package cn.com.jtv.rpc.client.login;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cn.com.jtv.rpc.proocal.LoginServiceProtocal;

public class LoginClient {
	
	public static void main(String[] args) throws Exception {
		
		LoginServiceProtocal proxy = RPC.getProxy(LoginServiceProtocal.class, 1L, new InetSocketAddress("localhost", 10000), new Configuration());
		
		String login = proxy.login("root", "hadoop");
		System.out.println(login);
	
	}

}
