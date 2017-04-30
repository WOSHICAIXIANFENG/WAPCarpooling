/**
 * 
 */
package com.carpool.service;

import java.util.List;

import com.carpool.dao.LikeDao;
import com.carpool.model.Like;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 25, 2017
 */
public class LikeService {

	private LikeDao likeDao;

	private static volatile LikeService instance;

	public static synchronized LikeService getInstance() {
		if (instance == null) {
			synchronized (LikeService.class) {
				if (instance == null)
					instance = new LikeService();
			}
		}
		return instance;
	}

	public LikeService() {
		likeDao = new LikeDao();
	}
	
	/*
	 * get all likes by the post Id
	 * 
	 */
	public int getLikesByPostId(int postId){
		return likeDao.getAllLikesByPostId(postId);
	}
	
	/*
	 * updats likes in the DB
	 * */
	public int updateLikeTable(int userid, int postid){
		return likeDao.insertLike(userid, postid);
	}
}
