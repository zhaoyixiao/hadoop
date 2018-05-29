package cn.com.jtv;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MyWatcher implements Watcher{
	private ZooKeeper zk;
	
	public MyWatcher() {
		
	}
	
	public MyWatcher(ZooKeeper zk) {
		this.zk = zk;
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			byte[] data = zk.getData("/eclipse", new MyWatcher(zk), null);
			
			System.out.println(event.getPath());
			System.out.println(event.getState());
			System.out.println(event.getType());
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
