package carpool_project;

import static org.junit.Assert.*;

import org.junit.Test;

import com.carpool.authentication.AuthenticationService;

public class AuthenticationTest {


	@Test
	public void testWrongEmail() {
		AuthenticationService as = new AuthenticationService();
		//boolean check = as.doCheck("test@test.com", "pass");
		//assertEquals(false, check);
	}
	
	@Test
	public void testCorrectEmail() {
		//AuthenticationService as = new AuthenticationService();
		//boolean check = as.doCheck("test@test.ru", "pass");
		//assertEquals(true, check);
	}

}
