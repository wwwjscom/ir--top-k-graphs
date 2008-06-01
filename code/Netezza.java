import java.sql.*;
import javax.sql.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.netezza.*;
import org.netezza.util.*;

//import org.jdom.*;
//import org.jdom.output.*;
//import org.jdom.xpath.* ;
//import org.jdom.input.SAXBuilder;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.*;
import org.apache.commons.pool.impl.*;

public class Netezza {
	private Object monitor;

	public static void main (String args[]) {
		String f = "";

		try {
			new Netezza(args, f);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Netezza (String args[], String file) throws Exception 
	{
		//String jdbcUrl = new String ("jdbc:mysql://192.168.1.56/graphs?user=nz&password=nz");
		String jdbcUrl = new String ("jdbc:netezza://192.168.1.56/graphs?user=admin&password=password");
		String jdbcClass = new String ("org.netezza.Driver");
		//String jdbcClass = new String ("org.gjt.mm.mysql.Driver");

		// Load the driver for this database
		Class.forName(jdbcClass).newInstance();

		int connectionPoolSize = (5 * 2) + 1;	
		ObjectPool connectionPool;
		connectionPool = new GenericObjectPool(null,              // factory
				connectionPoolSize,                       // maxActive
				GenericObjectPool.WHEN_EXHAUSTED_BLOCK,   // whenExhaustedAction
				-1,                                       // maxWait
				connectionPoolSize,                       // maxidle
				1,                                        // minIdle
				true,                                     // testOnBorrow
				false,                                    // testOnReturn
				3600000,                                  // timeBetweenEvictionRunMillis
				3,                                        // numTestsPerEvictionRun
				1800000,                                  // minEvictableIdleTimeMillis
				true);                                    // testWhileIdle

		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcUrl, null);
		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, // connFactory
				connectionPool,    // pool
				null,              // stmtPoolFactory
				"select 1",        // validationQuery
				false,             // defaultReadOnly
				true);             // defaultAutoCommit

		DataSource dataSource = new PoolingDataSource(connectionPool);
		final Connection c = dataSource.getConnection();	

		String name = "test_collection";
		int range = 0;
		int lifetime = 0;

		Statement s = c.createStatement();
		PreparedStatement ps = c.prepareStatement("SELECT * from attributes_2 LIMIT 10");
		ResultSet rs = ps.executeQuery();

		while (rs.next())
		{
			int v1 = rs.getInt(1);
			int v2 = rs.getInt(2);
			String attrName = rs.getString(3);
			String attrValue = rs.getString(4);

			System.out.println("Vertex 1: " + v1 + "\tVertex 2: " + v2 + "\tAttribute Name: " + attrName
							+ "\tAttribute Value: " + attrValue);
			System.out.println("");
		}

		//s.execute ("INSERT INTO collections (name, prange, plifetime, ptagpath) VALUES " +
				//" ('"+name+"', "+range+", "+lifetime+", 1)", Statement.RETURN_GENERATED_KEYS);

		c.close();
	}	   
}
