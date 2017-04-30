/**
 * 
 */
package com.carpool.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 23, 2017
 */
public class DBConnection {
	/**
	 * Comment by Xianfeng
	 * Why I need to add some appending info '?userUnicod....' ?
	 * In order to solve 'Unexpected exception resolving reference java.sql.SQLException: The server timezone value 'UTC' 
	 * http://stackoverflow.com/questions/26515700/mysql-jdbc-driver-5-1-33-time-zone-issue
	 */
	String dburl = "jdbc:mysql://localhost:3306/carpoolingdb?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&autoReconnect=true&useSSL=false";
	private static volatile DBConnection db;

	public DBConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException ex) {
			System.out.println("MySQL JDBC driver not found in DBConnection\n" + ex);
			System.exit(0);
		}
	}

	public static synchronized DBConnection getInstance() throws SQLException {
		if (db == null) {
			synchronized (DBConnection.class) {
				if (db == null) {
					db = new DBConnection();
				}
			}	
		}
		return db;
	}


	/**
	 * Get db connection by local db configuration 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(dburl, "root", "samuel");
	}
}
