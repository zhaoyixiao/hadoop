package cn.com.jtv.mydistserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * 客户端获取可用服务器
 * @author zhaoyx
 * @version 1.0
 *
 */
public class PayServiceConsumer {
	
	/**
	 * 用于存储可用服务器的列表
	 * volatile 的含义，在正式的环境中，查询可用服务器的方法和处理业务的方法在不同的线程中进行
	 * 也可以用线程同步锁
	 */
	private volatile List<String> servers = new ArrayList<String>();
	
	private ZooKeeper zk = null;
	
	
	public static void main(String[] args) throws Exception {
		
		PayServiceConsumer psc = new PayServiceConsumer();
		// 获取zk连接
		psc.connectZK();
		
		// 去zk上查询可用服务器
		psc.getOnlyServers();
		
		// 处理业务：挑选一台服务器发送业务请求
		psc.handleService();
		
	}
	
	public void connectZK() throws Exception{
		
		zk = new ZooKeeper("zk-01,zk-02,zk-03", 2000, new Watcher(){
			
			// 注册监听内部类 
			@Override
			public void process(WatchedEvent event) {
				//判断事件的类型（zk在启动的时候会执行一个空事件）
				if(event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged){}
				
				//重新获取可用的服务器，并重新获取监听
				try {
					getOnlyServers();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		});
	}
	
	
	/**
	 * 查询可用服务器
	 * @throws Exception 
	 */
	public void getOnlyServers() throws Exception{
		List<String> list = new ArrayList<String>();
		
		List<String> children = zk.getChildren("/servers", true);
		for (String child : children) {
			byte[] data = zk.getData("/servers/"+child, false, null);
			list.add(new String(data));
		}
		
		servers = list;
		for (String server : servers) {
			System.out.println("此刻可用的服务器有："+server);
		}
		System.out.println("--------------------------------------");
	}
	
	
	/**
	 * 处理业务的方法
	 * @throws Exception
	 */
	public void handleService() throws Exception{
		
		System.out.println("挑选了一台服务器来请求......");
		
		Thread.sleep(Long.MAX_VALUE);
		
	}

}
