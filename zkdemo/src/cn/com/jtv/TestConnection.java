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
 * zookeeper客户端测试类
 * 
 * @author zhaoyx
 * @version 1.0
 * 
 */
public class TestConnection {

	ZooKeeper zk = null;

	/**
	 * 创建数据节点
	 * 
	 * @throws Exception
	 * @throws KeeperException
	 */
	@Test
	public void createNode() throws KeeperException, Exception {
		// 参数1：创建的路径
		// 参数2：内容（2进制）
		// 参数3：权限
		// 参数4：节点的类型  如：-e   -p
		String create = zk.create("/eclipse", "eclipse是一个好工具！".getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		System.out.println(create);

	}
	
	
	/**
	 * 更改数据
	 */
	@Test
	public void setData() throws Exception{
		Stat stat = zk.setData("/eclipse", "up data eclipse ! ".getBytes(), -1);
		int version = stat.getVersion();
		System.out.println("version = " + version);
	}
	
	/**
	 * 删除zkNode
	 * @throws Exception
	 */
	@Test
	public void delNode()throws Exception{
		zk.delete("/eclipse", -1);
		
	}
	
	/**
	 * 判断zkNode是否存在
	 * @throws Exception
	 */
	@Test
	public void existNode()throws Exception{
		Stat stat = zk.exists("/eclipse", false);
		System.out.println(stat);
	}
	
	/**
	 * 获取子节点的名称，不带全路径
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
	 * 以对象的形式创建zkNode
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
	 * 注册监听，因为监听的时效性只有一次，所以此方法写的是永久监听
	 * @throws Exception
	 */
	@Test
	public void testWatcher() throws Exception{
		byte[] data = zk.getData("/eclipse", new MyWatcher(zk), null);
		System.out.println(new String(data));
		
		Thread.sleep(Long.MAX_VALUE);
		
	}
	

	/**
	 * 初始化zk
	 * 
	 * @author zhaoyx
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {
		// 设置连接多个节点，如果一个节点宕机，可以自动切换
		String conn = "zk-01:2181,zk-02:2181,zk-03:2181";

		// 参数二：连接超时时间，也就是心跳时间
		// 参数三：设置监听watch，如果不使用，为null
		zk = new ZooKeeper(conn, 2000, null);

		// 参数一：为数据的路径 // 参数二：为设置监听 //
		// 参数三：为数据的版本号，客户端一般都会获取最新版本，所以，参数可以设置为null，或new Stat() byte[] data =
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
