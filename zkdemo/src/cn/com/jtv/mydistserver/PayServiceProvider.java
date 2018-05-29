package cn.com.jtv.mydistserver;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ����������
 * @author zhaoyx
 * @version 1.0
 *
 */
public class PayServiceProvider {
	
	private ZooKeeper zk = null;
	
	public static void main(String[] args) throws Exception {
		PayServiceProvider psp = new PayServiceProvider();
		
		//��ȡzk�ͻ���
		psp.connectZK();
		
		//ע����Ϣ
		Stat exists = psp.zk.exists("/servers", false);
		//�ж��ļ����Ƿ���ڣ���������ڣ��������ýڵ� servers
		if(exists == null){
			psp.zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		// ������ʱ�ڵ㣻��Ϊ�������������ߵĻ����ͻ��˾�û�б�Ҫ�ٻ�ȡ����˵���Ϣ��
		psp.zk.create("/servers/server", args[0].getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		
		//����ҵ��
		psp.handleService();
		
	}
	
	
	/**
	 * zk�ͻ���
	 * @throws Exception
	 */
	public void connectZK() throws Exception{
		
		zk = new ZooKeeper("zk-01,zk-02,zk-03", 2000, null);
	}
	
	
	/**
	 * ���������
	 * @throws Exception
	 */
	public void handleService() throws Exception{
		
		System.out.println("�������Ϳ�ʼ����ҵ������");
		
		Thread.sleep(Long.MAX_VALUE);
		
	}
}
