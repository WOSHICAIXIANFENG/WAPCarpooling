/**
 * 
 */
package carpool_project.db;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Diana Yamaletdinova
 *
 * Apr 23, 2017
 */
public class DatabaseConnectionTest {
	
			
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
	public void checkForDriverTest() {
		
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            fail("MySQL JDBC driver not found in DBConnection\n" + ex);
        }
        assertTrue(true);
	}	
}
