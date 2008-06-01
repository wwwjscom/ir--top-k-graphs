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

		/* Query outer loop,  find all v1, the origin vertex */
		Statement s = c.createStatement();
		PreparedStatement ps = c.prepareStatement("SELECT VERTEX_1 from EDGES_2 ORDER BY CAST(VERTEX_1 as INT)");
		//PreparedStatement ps = c.prepareStatement("SELECT DISTINCT VERTEX_1 from EDGES_2");
		ResultSet rs = ps.executeQuery();


		/* Calculate the total number of v1's we have */
		final Connection xx = dataSource.getConnection();	
		Statement yy = xx.createStatement();
		PreparedStatement total = xx.prepareStatement("SELECT DISTINCT COUNT(VERTEX_1) from EDGES_2");
		ResultSet TOT = total.executeQuery();

		/* tracking int for debug statement */
		int debugPrint = 0;
		int TOTA = 0;
		while(TOT.next())
		{
			TOTA = TOT.getInt(1);
		}

		while (rs.next())
		{

			int v1 = rs.getInt(1);
			StringBuilder line = new StringBuilder();

			/* For each v1, find v2, the destionation vertex */
			final Connection cc = dataSource.getConnection();	
			Statement ss = cc.createStatement();
			PreparedStatement pss = cc.prepareStatement("SELECT VERTEX_2 from EDGES_2 WHERE VERTEX_1 = " + v1 + "");
			//System.out.println("SELECT COUNT(VERTEX_2) from EDGES_2 WHERE VERTEX_1 = " + v1 + ""); //DEBUG
			ResultSet rss = pss.executeQuery();

			/* for each set of v1 and v2, output the result */
			while (rss.next())
			{
				int v2a = rss.getInt(1);
				boolean Zero = true;


				final Connection ccc = dataSource.getConnection();	
				Statement sss = ccc.createStatement();
				PreparedStatement psss = ccc.prepareStatement("SELECT VERTEX_2 from EDGES_2 WHERE VERTEX_1 = " + v2a);
				ResultSet rsss = psss.executeQuery();




				while(rsss.next())
				{
					int v2b = rsss.getInt(1);

					if(v2b == v1) {
						// If already bidirectional then do nothing
						Zero = false;
					} else {
						// If not bidirectional then insert into the DB.  The vertexes are reversed because
						// we are making it bidirectional.
						//sss.execute ("INSERT INTO bidirectional (v1, v2) VALUES ('"+v2a+"', '"+v1+"')");
						Zero = true;
					}
					//Zero = false;
				}

				if(Zero) {
					// if there are zero results then we know instantly that it is not bidirectional
					// so we need to insert.
					sss.execute ("INSERT INTO bidirectional (v1, v2) VALUES ('"+v2a+"', '"+v1+"')");
				}

				ccc.close();
			}


			cc.close();

			if(debugPrint % 100 == 0)
			{
				System.out.println("*************************************");
				System.out.println("*************************************");
				System.out.println("\t\t" + debugPrint + " out of " + TOTA);
				System.out.println("*************************************");
				System.out.println("*************************************");
			}
			debugPrint++;
		}

		c.close();
	}	   
}
