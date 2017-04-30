package com.carpool.model;

import java.io.Serializable;
import java.util.Date;

/**
 * 
 * @author Xianfeng Cai
 */
public class Like implements Serializable {
	private static final long serialVersionUID = -8633926039369414936L;
	
	private int likeid;
	private int userid;
	private int postid;
	
	private Date dateCreated;
	private Date dateUpdated;
	
	public Like(int likeid, int userid, int postid, Date dateCreated, Date dateUpdated) {
		super();
		this.likeid = likeid;
		this.userid = userid;
		this.postid = postid;
		this.dateCreated = dateCreated;
		this.dateUpdated = dateUpdated;
	}

	public int getLikeid() {
		return likeid;
	}

	public void setLikeid(int likeid) {
		this.likeid = likeid;
	}

	public int getUserid() {
		return userid;
	}

	public void setUserid(int userid) {
		this.userid = userid;
	}

	public int getPostid() {
		return postid;
	}

	public void setPostid(int postid) {
		this.postid = postid;
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
