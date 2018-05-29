package cn.com.jtv;

import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

/**
 * zookeeper�ͻ��˲�����
 * 
 * @author zhaoyx
 * @version 1.0
 * 
 */
public class TestConnection {

	ZooKeeper zk = null;

	/**
	 * �������ݽڵ�
	 * 
	 * @throws Exception
	 * @throws KeeperException
	 */
	@Test
	public void createNode() throws KeeperException, Exception {
		// ����1��������·��
		// ����2�����ݣ�2���ƣ�
		// ����3��Ȩ��
		// ����4���ڵ������  �磺-e   -p
		String create = zk.create("/eclipse", "eclipse��һ���ù��ߣ�".getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(create);

	}
	
	
	/**
	 * ��������
	 */
	@Test
	public void setData() throws Exception{
		Stat stat = zk.setData("/eclipse", "up data eclipse ! ".getBytes(), -1);
		int version = stat.getVersion();
		System.out.println("version = " + version);
	}
	
	/**
	 * ɾ��zkNode
	 * @throws Exception
	 */
	@Test
	public void delNode()throws Exception{
		zk.delete("/eclipse", -1);
		
	}
	
	/**
	 * �ж�zkNode�Ƿ����
	 * @throws Exception
	 */
	@Test
	public void existNode()throws Exception{
		Stat stat = zk.exists("/eclipse", false);
		System.out.println(stat);
	}
	
	/**
	 * ��ȡ�ӽڵ�����ƣ�����ȫ·��
	 * @throws Exception
	 */
	@Test
	public void getChildrenNode()throws Exception{
		List<String> list = zk.getChildren("/", false);
		Iterator<String> it = list.iterator();
		while(it.hasNext()){
			String str = it.next();
			System.out.println(str);
		}
	}
	
	/**
	 * �Զ������ʽ����zkNode
	 * @throws Exception
	 */
	@Test
	public void setObjectIntoZk() throws Exception{
		
		Person person = new Person();
		person.setName("zhangsan");
		
		Gson gson = new Gson();
		String json = gson.toJson(person);
		String create = zk.create("/aaa/bbb", json.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		byte[] data = zk.getData("/aaa/bbb", false, null);
		String result = new String(data);
		
		Person person2 = gson.fromJson(result, Person.class);
		System.out.println("person2.name = " + person2.getName());
	}
	
	/**
	 * ע���������Ϊ������ʱЧ��ֻ��һ�Σ����Դ˷���д�������ü���
	 * @throws Exception
	 */
	@Test
	public void testWatcher() throws Exception{
		byte[] data = zk.getData("/eclipse", new MyWatcher(zk), null);
		System.out.println(new String(data));
		
		Thread.sleep(Long.MAX_VALUE);
		
	}
	

	/**
	 * ��ʼ��zk
	 * 
	 * @author zhaoyx
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {
		// �������Ӷ���ڵ㣬���һ���ڵ�崻��������Զ��л�
		String conn = "zk-01:2181,zk-02:2181,zk-03:2181";

		// �����������ӳ�ʱʱ�䣬Ҳ��������ʱ��
		// �����������ü���watch�������ʹ�ã�Ϊnull
		zk = new ZooKeeper(conn, 2000, null);

		// ����һ��Ϊ���ݵ�·�� // ��������Ϊ���ü��� //
		// ��������Ϊ���ݵİ汾�ţ��ͻ���һ�㶼���ȡ���°汾�����ԣ�������������Ϊnull����new Stat() byte[] data =
		// byte[] data = zk.getData("/aaa/bbb", false, new Stat());

		// System.out.println("data = " + new String(data));

	}

	@After
	public void close() throws Exception {
		zk.close();
	}

	public static void main(String[] args) throws Exception {
		String conn = "zk-01:2181,zk-02:2181,zk-03:2181";

		ZooKeeper zk = new ZooKeeper(conn, 2000, null);
		byte[] data = zk.getData("/aaa/bbb", false, new Stat());
		System.out.println("data = " + new String(data));

	}

}
