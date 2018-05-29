package cn.com.jtv.rpc.proocal;

public interface LoginServiceProtocal {
	public static final long versionID = 1L;
	
	String login(String userName, String password);

}
