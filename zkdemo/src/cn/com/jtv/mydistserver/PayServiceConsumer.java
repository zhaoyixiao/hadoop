package cn.com.jtv.mydistserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * �ͻ��˻�ȡ���÷�����
 * @author zhaoyx
 * @version 1.0
 *
 */
public class PayServiceConsumer {
	
	/**
	 * ���ڴ洢���÷��������б�
	 * volatile �ĺ��壬����ʽ�Ļ����У���ѯ���÷������ķ����ʹ���ҵ��ķ����ڲ�ͬ���߳��н���
	 * Ҳ�������߳�ͬ����
	 */
	private volatile List<String> servers = new ArrayList<String>();
	
	private ZooKeeper zk = null;
	
	
	public static void main(String[] args) throws Exception {
		
		PayServiceConsumer psc = new PayServiceConsumer();
		// ��ȡzk����
		psc.connectZK();
		
		// ȥzk�ϲ�ѯ���÷�����
		psc.getOnlyServers();
		
		// ����ҵ����ѡһ̨����������ҵ������
		psc.handleService();
		
	}
	
	public void connectZK() throws Exception{
		
		zk = new ZooKeeper("zk-01,zk-02,zk-03", 2000, new Watcher(){
			
			// ע������ڲ��� 
			@Override
			public void process(WatchedEvent event) {
				//�ж��¼������ͣ�zk��������ʱ���ִ��һ�����¼���
				if(event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged){}
				
				//���»�ȡ���õķ������������»�ȡ����
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
	 * ��ѯ���÷�����
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
			System.out.println("�˿̿��õķ������У�"+server);
		}
		System.out.println("--------------------------------------");
	}
	
	
	/**
	 * ����ҵ��ķ���
	 * @throws Exception
	 */
	public void handleService() throws Exception{
		
		System.out.println("��ѡ��һ̨������������......");
		
		Thread.sleep(Long.MAX_VALUE);
		
	}

}
