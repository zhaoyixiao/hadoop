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
 * HDFS�Ŀͻ���api ����ȥ����hdfs���ļ����ļ���
 * 
 * @author sync
 *
 */
public class test {

	private FileSystem fs;
	
	/**
	 * д���ļ���hdfs��
	 * @throws Exception
	 */
	@Test
	public void writeDataToHdfs()throws Exception{
		
		/**
		 * ����2���Ƿ񸲸�Դ�ļ�
		 * 
		 */
		FSDataOutputStream out = fs.create(new Path("/access.log"), false);
		out.write("6666666666666666".getBytes());
		out.write("8888888888888888".getBytes());
		out.flush();
		out.close();
	}
	
	/**
	 * ��ȡ�ļ���ָ��ƫ������Χ������
	 * ��������ļ���ÿ�ζ�ȡ�Ĵ�С����Ϊblock��Ĵ�С���Խ�ʡ��Դ����Ϊ�ļ��ϴ���hdfs�ϵ�ʱ���ֿ���д洢��
	 * �����ȡ�ļ��Ĵ�С����һ��block��Ĵ�С����Ҫռ������datanode�ڵ����Դ
	 * �ֲ�ʽ�����������ʵ��������������������ϣ�����ʵ�����Զ�ȡhdfs���ļ�������ָ����Χ
	 * @throws Exception
	 */
	@Test
	public void readFilePart()throws Exception{
		FSDataInputStream in = fs.open(new Path("/a.gz"));
		in.seek(0);
		BufferedInputStream bfin = new BufferedInputStream(in);
		//ָ����ʼ��ȡƫ����
		
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
	 * ��ȡָ��·���µ������ļ��л��ļ��ڵ���Ϣ
	 * @throws Exception
	 */
	@Test
	public void listdir2()throws Exception{
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus file : listStatus) {
			//�ļ���·��
			System.out.println(file.getPath());
			//�Ƿ�Ϊ�ļ���
			System.out.println(file.isDirectory());
			
		}
	}
	
	/**
	 * �ݹ��ѯָ��·���µ��ļ���Ϣ
	 * @throws Exception
	 */
	@Test
	public void listdir() throws Exception{
		
		/**
		 * ����1����ȡ·��
		 * ����2���Ƿ�ݹ��������Ŀ¼
		 * 
		 * ���ص��ǵ�����������ֱ�ӷ��ؼ��ϣ�ԭ�������Ŀ¼�´洢�˴��������ݣ�������ؼ��ϵĻ�Ҫռ�÷ǳ�����ڴ�ռ䣬����ֻ���ص�����
		 */
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while(files.hasNext()){
			LocatedFileStatus file = files.next();
			System.out.println("����ķ���ʱ�䣺"+file.getAccessTime());
			System.out.println("blocksize:"+file.getBlockSize());
			System.out.println("�ļ�������:"+file.getGroup());
			System.out.println("�ļ��ܳ���:"+file.getLen());
			System.out.println("�ļ�����޸�ʱ��:"+file.getModificationTime());
			System.out.println("�ļ�������:"+file.getOwner());
			System.out.println("�ļ�������:"+file.getReplication());
			System.out.println("�ļ����λ����Ϣ:"+Arrays.toString(file.getBlockLocations()));
			System.out.println("�ļ���ȫ·����"+file.getPath());
			System.out.println("�ļ���Ȩ����Ϣ��"+file.getPermission());
			System.out.println("-------------------------------------------------");
			//break;
		}
	}
	
	
	/**
	 * �޸��ļ�������
	 * @throws Exception
	 */
	@Test
	public void renamedir()throws Exception{
		
		/**
		 * ����1��ԭ���ļ�����
		 * ����2���޸ĺ���ļ�����
		 */
		fs.rename(new Path("/test1"), new Path("/test2"));
	} 
	
	/**
	 * ɾ���ļ���
	 * @throws Exception
	 */
	@Test
	public void deldri()throws Exception{
		
		/**
		 * ����1��ɾ���ļ���·��
		 * ����2���ݹ������ɾ�����ļ���
		 * 
		 */
		fs.delete(new Path("/result"), true);
	}
	
	/**
	 * �����ļ���
	 * @throws Exception
	 */
	@Test
	public void mkdir()throws Exception{
		
		//��������ļ����ǣ�ָ����Ȩ����Ϣ��������������ָ����һ�£���Ϊ�ƶ���лл���ᾭ��һ������ֵ����������
		//��hadoop-site.xml�����ļ��� fs.permissions.umask-mode������Ĭ��ֵ��022�������Ĭ������²��Ḳ�ǵ����������Ա��wȨ��
		fs.mkdirs(new Path("/result"), new FsPermission((short)777));
	}

	/**
	 * ��ȡ�ļ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void getFile() throws Exception {

		/**
		 * ����1���Ƿ�ɾ��������Դ�ļ� ����
		 * 2���ļ���������ַ ����
		 * 3���ͻ��˵�ַ ����
		 * 4�����Ϊtrue����ʹ��javaԭ���⣬���Ϊfalse����ʹ��hadoop�Լ��ı��ؿ�
		 * 	    ���ʹ�ñ��ؿ⣬��Ҫ������hadoop�������أ���binĿ¼���õ�����������
		 */

		fs.copyToLocalFile(false, new Path("/GZPhone.zip"), new Path("d:/"), true);
	}

	/**
	 * �ϴ��ļ�
	 * 
	 * @throws Exception
	 */
	@Test
	public void putFile() throws Exception {
		// �ϴ��ļ�
		fs.copyFromLocalFile(new Path("E:/GZPhone.zip"), new Path("/GZPhone.zip"));
	}

	/**
	 * �رտͻ���
	 * @throws Exception
	 */
	@After
	public void close() throws Exception {
		fs.close();
	}

	/**
	 * ��ʼ���ͻ���
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception {

		/**
		 * uri:�yһ�YԴ��λ�� conf:�͑������ã��൱������hdfs-site.xml ���property��������;
		 * conf������hdfs���ԣ��൱������hadoop-site.xml
		 * user:��������¼��Ա
		 * 
		 */
		Configuration conf = new Configuration();
		// ��һ��ʵ�֣�����xml�����ļ�
		conf.addResource("myconf.xml");
		// �ڶ���ʵ�֣�conf.set()
		conf.set("dfs.replication", "2");
		conf.set("dfs.blocksize", "128m");
		fs = FileSystem.get(new URI("hdfs://hdp-nn-01:9000/"), conf, "root");

		// ���ʱ��ش���
		// FileSystem fs = FileSystem.get(new URI("file:///d:/"), conf, "root");
	}

	public static void main(String[] args) throws Exception {
		// init();
		// �رտͻ���
		// fs.close();

	}
}
