/**
 * 
 */
package com.carpool.model;

import java.util.Date;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 23, 2017
 */
public class Post {
	
	private int postId;
	private int userId;
	private String postBody;
	private int postType;
	private Date dateCreated;
	private Date dateUpdated;
	private String fullname = "";
	
	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
	}

	public int getPostId() {
		return postId;
	}

	public void setPostId(int postId) {
		this.postId = postId;
	}
	
	public Post( int postId, int userId, String post, int postType, Date date, Date date2) {
		this.postId = postId;
		this.userId = userId;
		this.postBody = post;
		this.postType = postType;
		this.dateCreated = date;
		this.dateUpdated = date2;	
	}
	
	public Post( int postId, int userId, String post, int postType, Date date, Date date2, String fname) {
		this.postId = postId;
		this.userId = userId;
		this.postBody = post;
		this.postType = postType;
		this.dateCreated = date;
		this.dateUpdated = date2;	
		this.fullname = fname;
	}
	
	public Post(){}

	public int getUserId() {
		return userId;
	}
	public void setUserId(int userId) {
		this.userId = userId;
	}
	public String getPostBody() {
		return postBody;
	}
	public void setPostBody(String post) {
		this.postBody = post;
	}
	public int getPostType() {
		return postType;
	}
	public void setPostType(int type) {
		this.postType = type;
	}
	public Date getDateCreated() {
		return dateCreated;
	}
	public void setDateCreated(Date cateCreated) {
		this.dateCreated = cateCreated;
	}
	public Date getDateUpdated() {
		return dateUpdated;
	}
	public void setDateUpdated(Date dateUpdated) {
		this.dateUpdated = dateUpdated;
	}
	
	
}
