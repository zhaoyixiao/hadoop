package demo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS的客户端api 就是去操作hdfs的文件或文件夹
 * 
 * @author sync
 *
 */
public class test {

	private FileSystem fs;
	
	/**
	 * 写入文件到hdfs中
	 * @throws Exception
	 */
	@Test
	public void writeDataToHdfs()throws Exception{
		
		/**
		 * 参数2：是否覆盖源文件
		 * 
		 */
		FSDataOutputStream out = fs.create(new Path("/access.log"), false);
		out.write("6666666666666666".getBytes());
		out.write("8888888888888888".getBytes());
		out.flush();
		out.close();
	}
	
	/**
	 * 读取文件中指定偏移量范围的数据
	 * 如果读大文件，每次读取的大小尽量为block块的大小，以节省资源，因为文件上传到hdfs上的时候会分块进行存储，
	 * 如果读取文件的大小不是一个block块的大小，就要占用两个datanode节点的资源
	 * 分布式运算程序，运行实例可以运行在任意机器上，任意实例可以读取hdfs中文件的任意指定范围
	 * @throws Exception
	 */
	@Test
	public void readFilePart()throws Exception{
		FSDataInputStream in = fs.open(new Path("/a.gz"));
		in.seek(0);
		BufferedInputStream bfin = new BufferedInputStream(in);
		//指定初始读取偏移量
		
		FileOutputStream out = new FileOutputStream("d:/hadooptest/a-04.gz");
		BufferedOutputStream bfout = new BufferedOutputStream(out);
		byte[] b = new byte[1024];
		int len = 0;
		long count = 0;
		while((len = bfin.read(b))!=-1){
			bfout.write(b);
			count += len;
			if(count == (1024*1024*128)){
				System.out.println(count);
				break;
			}
		}
		bfout.flush();
		bfout.close();
		out.close();
		bfin.close();
		in.close();
	}
	
	
	/**
	 * 获取指定路径下的所有文件夹或文件节点信息
	 * @throws Exception
	 */
	@Test
	public void listdir2()throws Exception{
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus file : listStatus) {
			//文件夹路径
			System.out.println(file.getPath());
			//是否为文件夹
			System.out.println(file.isDirectory());
			
		}
	}
	
	/**
	 * 递归查询指定路径下的文件信息
	 * @throws Exception
	 */
	@Test
	public void listdir() throws Exception{
		
		/**
		 * 参数1：获取路径
		 * 参数2：是否递归出所有子目录
		 * 
		 * 返回的是迭代器，而不直接返回集合，原因是如果目录下存储了大量的数据，如果返回集合的话要占用非常大的内存空间，所以只返回迭代器
		 */
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while(files.hasNext()){
			LocatedFileStatus file = files.next();
			System.out.println("最近的访问时间："+file.getAccessTime());
			System.out.println("blocksize:"+file.getBlockSize());
			System.out.println("文件所属组:"+file.getGroup());
			System.out.println("文件总长度:"+file.getLen());
			System.out.println("文件最近修改时间:"+file.getModificationTime());
			System.out.println("文件所属者:"+file.getOwner());
			System.out.println("文件副本数:"+file.getReplication());
			System.out.println("文件块的位置信息:"+Arrays.toString(file.getBlockLocations()));
			System.out.println("文件的全路径："+file.getPath());
			System.out.println("文件的权限信息："+file.getPermission());
			System.out.println("-------------------------------------------------");
			//break;
		}
	}
	
	
	/**
	 * 修改文件夹名称
	 * @throws Exception
	 */
	@Test
	public void renamedir()throws Exception{
		
		/**
		 * 参数1：原有文件名称
		 * 参数2：修改后的文件名称
		 */
		fs.rename(new Path("/test1"), new Path("/test2"));
	} 
	
	/**
	 * 删除文件夹
	 * @throws Exception
	 */
	@Test
	public void deldri()throws Exception{
		
		/**
		 * 参数1：删除文件夹路径
		 * 参数2：递归操作，删除子文件夹
		 * 
		 */
		fs.delete(new Path("/result"), true);
	}
	
	/**
	 * 创建文件夹
	 * @throws Exception
	 */
	@Test
	public void mkdir()throws Exception{
		
		//如果创建文件夹是，指定的权限信息，但结果并不会跟指定的一致，因为制定的谢谢还会经过一个参数值的掩码运算
		//在hadoop-site.xml配置文件中 fs.permissions.umask-mode参数，默认值是022，因此在默认情况下不会覆盖掉组和其他成员的w权限
		fs.mkdirs(new Path("/result"), new FsPermission((short)777));
	}

	/**
	 * 获取文件
	 * 
	 * @throws Exception
	 */
	@Test
	public void getFile() throws Exception {

		/**
		 * 参数1：是否删除服务器源文件 参数
		 * 2：文件服务器地址 参数
		 * 3：客户端地址 参数
		 * 4：如果为true，则使用java原生库，如果为false，则使用hadoop自己的本地库
		 * 	    如果使用本地库，需要下载在hadoop官网下载，将bin目录配置到环境变量中
		 */

		fs.copyToLocalFile(false, new Path("/GZPhone.zip"), new Path("d:/"), true);
	}

	/**
	 * 上传文件
	 * 
	 * @throws Exception
	 */
	@Test
	public void putFile() throws Exception {
		// 上传文件
		fs.copyFromLocalFile(new Path("E:/GZPhone.zip"), new Path("/GZPhone.zip"));
	}

	/**
	 * 关闭客户端
	 * @throws Exception
	 */
	@After
	public void close() throws Exception {
		fs.close();
	}

	/**
	 * 初始化客户端
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {

		/**
		 * uri:統一資源定位符 conf:客戶端配置，相当于设置hdfs-site.xml 里的property参数属性;
		 * conf：配置hdfs属性，相当于设置hadoop-site.xml
		 * user:服务器登录人员
		 * 
		 */
		Configuration conf = new Configuration();
		// 第一种实现：加载xml配置文件
		conf.addResource("myconf.xml");
		// 第二种实现：conf.set()
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "128m");
		fs = FileSystem.get(new URI("hdfs://hdp-nn-01:9000/"), conf, "root");

		// 访问本地磁盘
		// FileSystem fs = FileSystem.get(new URI("file:///d:/"), conf, "root");
	}

	public static void main(String[] args) throws Exception {
		// init();
		// 关闭客户端
		// fs.close();

	}
}
