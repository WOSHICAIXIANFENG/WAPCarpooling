/**
 * 
 */
package carpool_project.db;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Diana Yamaletdinova
 *
 * Apr 23, 2017
 */
public class UserDaoTest {
	
	String dburl = "jdbc:mysql://localhost:3306/carpoolingdb";

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void getUsersTest() throws Exception {
		
		Connection conn = DriverManager.getConnection(dburl,"carpoolapp","carpoolapp");
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT userid, fullname, gender FROM users");
		
		int countRows = 0;
		
		while (rs.next()) {
			System.out.println(rs.getInt("userId"));
			System.out.println(rs.getString(2));
			System.out.println(rs.getInt("gender"));
			countRows++;
		}
		rs.close();
		stmt.close();
		conn.close();

		assertEquals("Number of rows is 3", 3, countRows);;
	}

}
