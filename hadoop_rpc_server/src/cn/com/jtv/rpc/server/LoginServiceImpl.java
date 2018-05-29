package cn.com.jtv.rpc.server;

import cn.com.jtv.rpc.proocal.LoginServiceProtocal;

public class LoginServiceImpl implements LoginServiceProtocal{
	
	
	@Override
	public String login(String userName,String password){
		
		//里面写业务逻辑
		
		return "success";
	}

}
