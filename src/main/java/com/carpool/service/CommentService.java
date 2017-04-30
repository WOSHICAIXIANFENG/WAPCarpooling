/**
 * 
 */
package com.carpool.service;

import java.util.List;

import com.carpool.dao.CommentDao;
import com.carpool.model.Comment;
import com.carpool.util.ResultBean;

/**
 * @author Diana Yamaletdinova
 *
 * Apr 24, 2017
 */
public class CommentService {

	private CommentDao commentDao;

	private static volatile CommentService instance; 
	
	public static synchronized CommentService getInstance(){
		if(instance ==null){
			synchronized (CommentService.class) {
				if(instance == null )
					instance = new CommentService();				
			}
		}
		return instance; 
}
	public CommentService() {
		commentDao = new CommentDao();
	}
	
	/*
	 * get all existing comments
	 */
	public List<Comment> getAllComments(){
		return commentDao.getAllCommentsFromDb();
	}
	
	/*
	 * get all comments by the post Id
	 * 
	 */
	public List<Comment> getCommentsByPostId(int postId){
		return commentDao.getCommentsByPostId(postId);
	}
	
	/*
	 * add one comment
	 * 
	 */
	public ResultBean addOneComment(int userId, int postId, String comm) {
		
		List<Comment> comByUserIdList = commentDao.getCommentsByUserId(userId);
		
		Comment comment = new Comment();
		comment.setUserId(userId);
		comment.setPostId(postId);
		comment.setCommentBody(comm);
		boolean result = commentDao.addOneComment(comment);
		
		if(result) {
			ResultBean bean = new ResultBean (true, "Comment added successfully.");
			bean.setObj(comment);
			return bean;
		} else {
			return new ResultBean(false, "Comment was not added, please try again later.");
		}
	}
	
	/*
	 * delete one comment by user id
	 * 
	 */
	public ResultBean deleteCommentByUserId(int userId, int comId) {
		
		List<Comment> comByUserIdList = commentDao.getCommentsByUserId(userId);
	
		boolean result = commentDao.deleteCommentByUserID(userId, comId);
		
		if(result) {
			ResultBean bean = new ResultBean (true, "Comment deleted successfully.");
			bean.setObj(comByUserIdList);
			return bean;
		} else {
			return new ResultBean(false, "Comment was not deleted, please try again later.");
		}
	}
}
