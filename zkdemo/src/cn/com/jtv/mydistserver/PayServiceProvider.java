package cn.com.jtv.mydistserver;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 服务器功能
 * @author zhaoyx
 * @version 1.0
 *
 */
public class PayServiceProvider {
	
	private ZooKeeper zk = null;
	
	public static void main(String[] args) throws Exception {
		PayServiceProvider psp = new PayServiceProvider();
		
		//获取zk客户端
		psp.connectZK();
		
		//注册信息
		Stat exists = psp.zk.exists("/servers", false);
		//判断文件夹是否存在，如果不存在，创建永久节点 servers
		if(exists == null){
			psp.zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// 创建临时节点；因为当服务端如果掉线的话，客户端就没有必要再获取服务端的信息了
		psp.zk.create("/servers/server", args[0].getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		
		//处理业务
		psp.handleService();
		
	}
	
	
	/**
	 * zk客户端
	 * @throws Exception
	 */
	public void connectZK() throws Exception{
		
		zk = new ZooKeeper("zk-01,zk-02,zk-03", 2000, null);
	}
	
	
	/**
	 * 开启服务端
	 * @throws Exception
	 */
	public void handleService() throws Exception{
		
		System.out.println("服务器就开始接受业务请求");
		
		Thread.sleep(Long.MAX_VALUE);
		
	}
}
