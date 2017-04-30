/**
 * 
 */
package com.carpool.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carpool.model.Post;


/**
 * @author Diana Yamaletdinova
 *
 * Apr 23, 2017
 */
public class MockDao {
Map<Integer, Post> postDb = new HashMap<>();
	
	/*{
		postDb.put(1, new Post(1, 1, "Ride to LA", "offer", "04/05/17/", "04/06/17/"));
		postDb.put(2, new Post(2, 2, "Ride to DSM", "need", "04/07/17/", "04/08/17/"));
		postDb.put(3, new Post(2, 3, "Ride to NY", "offer", "04/11/17/", "04/15/17/"));
		postDb.put(4, new Post(2, 3, "Ride to CA", "need", "04/11/17/", "04/15/17/"));

	}*/
	
	/*public void addContact(Post post){
		contactDb.put(post.getContactId(), contact);
	}
	
	public void deleteContact(int contactId){
		contactDb.remove(contactId);
	}
	
	public void updateContact(Contact contact){
		contactDb.put(contact.getContactId(), contact);
	}
	*/
	public List<Post> getAllPosts(){
		return new ArrayList<>(postDb.values());
	}
	
	public Post getPOstById(int postId){
		return postDb.get(postId);
	}

	public int genId() {
		return postDb.size()+1;
	}
}
