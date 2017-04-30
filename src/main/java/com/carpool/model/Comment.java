/**
 * 
 */
package com.carpool.model;

import java.util.Date;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 24, 2017
 */
public class Comment {

	private int commentId;
	private int userId;
	private int postId;
	private String commentBody;
	private Date dateCreated;
	private Date dateUpdated;

	public Comment(int commentId, int userId, int postId, String commentBody, Date dateCreated, Date dateUpdated) {
		this.commentId = commentId;
		this.userId = userId;
		this.postId = postId;
		this.commentBody = commentBody;
		this.dateCreated = dateCreated;
		this.dateUpdated = dateUpdated;
	}
	
	public Comment(){}

	public int getCommentId() {
		return commentId;
	}

	public void setCommentId(int commentId) {
		this.commentId = commentId;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getPostId() {
		return postId;
	}

	public void setPostId(int postId) {
		this.postId = postId;
	}

	public String getCommentBody() {
		return commentBody;
	}

	public void setCommentBody(String commentBody) {
		this.commentBody = commentBody;
	}

	public Date getDateCreated() {
		return dateCreated;
	}

	public void setDateCreated(Date dateCreated) {
		this.dateCreated = dateCreated;
	}

	public Date getDateUpdated() {
		return dateUpdated;
	}

	public void setDateUpdated(Date dateUpdated) {
		this.dateUpdated = dateUpdated;
	}


}
