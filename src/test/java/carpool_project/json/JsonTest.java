/**
 * 
 */
package carpool_project.json;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
/**
 * @author Diana Yamaletdinova
 *
 * Apr 24, 2017
 */
public class JsonTest  extends TestCase{
	/**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JsonTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JsonTest.class );
    }

    /**
     * Rigourous Test :-)
     * @throws JsonProcessingException 
     */
    public void testApp() throws JsonProcessingException
    {
    	// This object comes from the service with data from the database.
    	List<Example> exList = new ArrayList<Example>();
    	
    	exList.add(new Example("Winny", 21, "winny@mail.ru"));
    	exList.add(new Example("Majestic", 24, "majestic@mail.ru"));
    	exList.add(new Example("Sonya", 14, "winny@mail.ru"));
    	
    	exList.stream().forEach(System.out::println);
    	
    	// This code is in your servlet to convert the list from the service to json and send
    	// to browser where javascript will load as a javascript array.
    	ObjectMapper mapper = new ObjectMapper();
    	
    	String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(exList);
    	
    	System.out.println(json);
    	
        assertTrue( true );
    }
    
// Javascript can read the json directly into javascript objects.
    
    //  How to read in the json list
    //http://stackoverflow.com/questions/9991805/javascript-how-to-parse-json-array
    
    /*
    var jsonData = JSON.parse("this is the json data sent from server");

	// JsonData is your json data as a javascript array list.
	//
	for (var i = 0; i < jsonData.counters.length; i++) {
    	var counter = jsonData.counters[i];
    	console.log(counter.counter_name);
	}
    
    */
    class Example {
    	private String name;
    	private int age;
    	private String email;
    	
		public Example(String name, int age, String email) {
			super();
			this.name = name;
			this.age = age;
			this.email = email;
		}
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		public String getEmail() {
			return email;
		}
		public void setEmail(String email) {
			this.email = email;
		}
		
		@Override
		public String toString() {
			return "Name = " + name + ", age = " + age + ", email = " + email;
		}
    }

}
