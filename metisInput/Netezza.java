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

		/*
		// Line number reader, used for jumping directly to a line number since METIS requires that for input
		LineNumberReader LNR = new LineNumberReader(new BufferedReader(new FileReader("FEED_ME_TO_METIS")));
		//LineNumberReader LNR = new LineNumberReader(new FileReader("FEED_ME_TO_METIS"));
		*/

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

		/* Calculate the total number of edges we have */
		final Connection Exx = dataSource.getConnection();	
		Statement Eyy = Exx.createStatement();
		PreparedStatement Etotal = Exx.prepareStatement("select COUNT(*) from EDGES_2;");
		ResultSet ETOT = Etotal.executeQuery();


		/* tracking int for debug statement */
		int debugPrint = 0;
		int TOTA = 0;
		while(TOT.next())
		{
			TOTA = TOT.getInt(1);
		}

		int EETOT = 0;
		while(ETOT.next())
		{
			EETOT = ETOT.getInt(1);
		}

		/* Write the first line to the file which is required to have the total num of Vertexes and Edges on it */
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter("FEED_ME_TO_METIS",true));
			out.write(TOTA + " " + EETOT + " " + "\n");
			out.close();
		} catch (IOException e) {
		}


		while (rs.next())
		{

			int v1 = rs.getInt(1);
			StringBuilder line = new StringBuilder();

			/* For each v1, find v2, the destionation vertex */
			final Connection cc = dataSource.getConnection();	
			Statement ss = cc.createStatement();
			PreparedStatement pss = cc.prepareStatement("SELECT DISTINCT COUNT(VERTEX_2) from EDGES_2 WHERE VERTEX_1 = " + v1 + "");
			//PreparedStatement pss = cc.prepareStatement("SELECT COUNT(VERTEX_2) from EDGES_2 WHERE VERTEX_1 = " + v1 + "");
			//System.out.println("SELECT COUNT(VERTEX_2) from EDGES_2 WHERE VERTEX_1 = " + v1 + ""); //DEBUG
			ResultSet rss = pss.executeQuery();

			final Connection ccc = dataSource.getConnection();	
			Statement sss = ccc.createStatement();
			PreparedStatement psss = ccc.prepareStatement("SELECT DISTINCT VERTEX_2 from EDGES_2 WHERE VERTEX_1 = " + v1 + "");
			//PreparedStatement psss = ccc.prepareStatement("SELECT VERTEX_2 from EDGES_2 WHERE VERTEX_1 = " + v1 + "");
			ResultSet rsss = psss.executeQuery();


			/* for each set of v1 and v2, output the result */
			while (rss.next())
			{

				int count = rss.getInt(1);
				//int v2b = rss.getInt(2);

				//String attrName = rss.getString(3);
				//String attrValue = rss.getString(4);

				//System.out.println("Vertex 1: " + v1 + "\tVertex 2: " + v2 + "\tAttribute Name: " + attrName
				//				+ "\tAttribute Value: " + attrValue);

				/* UNCOMMENT */
				//System.out.print(" " + v1 + " ");
				//line.append(v1 + " ");

				while((count != 0) && rsss.next())
				{
					int v2 = rsss.getInt(1);
					/* UNCOMMENT */
					//System.out.print(v2 + " ");
					line.append( v2 + " ");
				}

				/* UNCOMMENT */
				//System.out.println("");

			}


			ccc.close();
			cc.close();

			try {
				BufferedWriter out = new BufferedWriter(new FileWriter("FEED_ME_TO_METIS",true));
				out.write(line.toString() + "\n");
				out.close();
			} catch (IOException e) {
			}

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

		//s.execute ("INSERT INTO collections (name, prange, plifetime, ptagpath) VALUES " +
				//" ('"+name+"', "+range+", "+lifetime+", 1)", Statement.RETURN_GENERATED_KEYS);

		c.close();
	}	   
}
