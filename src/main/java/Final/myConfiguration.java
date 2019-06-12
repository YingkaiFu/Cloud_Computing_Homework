package Final;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Configuration����
 * @author weili wang
 * 2013-6-16
 */
public class myConfiguration {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		conf.addResource(new Path("testdata/conf1.xml"));
		conf.set("name", "apple");
		
		System.out.println(conf.get("color"));
		System.out.println(conf.getInt("num", 1));
		System.out.println(conf.get("name"));
	}

}
