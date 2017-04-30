/**
 * 
 */
package com.carpool.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.carpool.dao.PostDao;
import com.carpool.model.Comment;
import com.carpool.model.Post;
import com.carpool.util.ResultBean;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 24, 2017
 */
public class PostService {

	private PostDao postDao;

	private static volatile PostService instance; 
	
	public static synchronized PostService getInstance(){
		if(instance ==null){
			synchronized (PostService.class) {
				if(instance == null )
					instance = new PostService();				
			}
		}
		return instance; 
}
	public PostService() {
		postDao = new PostDao();
	}

	public List<Post> getAllPosts() {
		return postDao.getAllPostsFormDb();
	}
	
	public List<Post> getAllPostsByType(int type) {
		return postDao.getPostsByType(type);
	}
	
	public List<Post> getAllPostsByUserId(int userId){
		return postDao.getPostsByUserId(userId);
	}
	
	public ResultBean deleteOnePostByPost (int postId){
		//List<Post> postsByUserIdList = postDao.getPostsByUserId(userId);
		Post post = new Post();
		post.setPostId(postId);
		boolean result = postDao.deletePostByUserID(postId);
		
		if(result) {
			ResultBean bean = new ResultBean (true, "Comment added successfully.");
			//bean.setObj(postsByUserIdList);
			return bean;
		} else {
			return new ResultBean(false, "Comment was not added, please try again later.");
		}
	}
	/**
	 * @param userid
	 * @param pickUpLocat
	 * @param destinatLocat
	 * @return
	 */
	public ResultBean addOnePost(int userid, String pickUpLocat, String destinatLocat, int type) {
		String postBody = "";
		if (type == 1){
			//request a ride
			postBody = "I need a ride from " + pickUpLocat + " to " + destinatLocat;
		}else if (type == 2){
			//ofer a ride
			postBody = "I am offering a ride from " + pickUpLocat + " to " + destinatLocat;
		}
		List<Post> comByUserIdList = postDao.getPostsByUserId(userid);
		
		Post post = new Post();
		post.setUserId(userid);
		post.setPostBody(postBody);
		post.setPostType(type);
		boolean result = postDao.addOnePost(post);
		
		if(result) {
			ResultBean bean = new ResultBean (true, "Comment added successfully.");
			bean.setObj(comByUserIdList);
			return bean;
		} else {
			return new ResultBean(false, "Comment was not added, please try again later.");
		}
	}
}
