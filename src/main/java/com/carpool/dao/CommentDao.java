/**
 * 
 */
package com.carpool.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.carpool.jdbc.DBConnection;
import com.carpool.model.Comment;
import com.mysql.cj.jdbc.PreparedStatement;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 24, 2017
 */
public class CommentDao {

	public List<Comment> getAllCommentsFromDb() {
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		List<Comment> commentsList = new ArrayList<>();

		try {

			conn = DBConnection.getInstance().getConnection();
			//conn = DriverManager.getConnection(dburl, "carpoolapp", "carpoolapp");
			stmt = conn.createStatement();
			// When working on date types, the latest date is shown on top of
			// the list - DESC
			// SELECT * FROM posts ORDER BY datecreated DESC LIMIT 25;
			rs = stmt.executeQuery("SELECT * FROM comments");
			
			Comment comment = null;
			
			while (rs.next()) {
				comment = new Comment(
				rs.getInt(1),
				rs.getInt(2),
				rs.getInt(3),
				rs.getString(4),
				rs.getTimestamp(5),
				rs.getTimestamp(6)
				);
				commentsList.add(comment);
			}

		} catch (SQLException e) {

			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException ignore) {
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ignore) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException ignore) {
				}
			}
		}

		return commentsList;
	}

	public List<Comment> getCommentsByPostId(int postId){
		
		List<Comment> commentByPostIdList = new ArrayList<>();
		Connection conn = null;
		PreparedStatement prepstmt = null;
		String sql = "SELECT * FROM comments where postid = ?";
		ResultSet rs = null;
		
		try {

			conn = DBConnection.getInstance().getConnection();
			prepstmt = (PreparedStatement)conn.prepareStatement(sql);
			prepstmt.setInt(1, postId);
			rs =prepstmt.executeQuery(); 
			
			Comment comment = null;
			while (rs.next()) {
				comment = new Comment(
				rs.getInt(1),
				rs.getInt(2),
				rs.getInt(3),
				rs.getString(4),
				rs.getTimestamp(5),
				rs.getTimestamp(6)
				);
				commentByPostIdList.add(comment);
			}

		} catch (SQLException e) {

			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException ignore) {
				}
			}
			if (prepstmt != null) {
				try {
					prepstmt.close();
				} catch (SQLException ignore) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException ignore) {
				}
			}
		}

		return commentByPostIdList;
	}
	
public List<Comment> getCommentsByUserId(int userId){
		
		List<Comment> commentByUserIdList = new ArrayList<>();
		Connection conn = null;
		PreparedStatement prepstmt = null;
		String sql = "SELECT * FROM comments where userid = ?";
		ResultSet rs = null;
		
		try {

			conn = DBConnection.getInstance().getConnection();
			prepstmt = (PreparedStatement)conn.prepareStatement(sql);
			prepstmt.setInt(1, userId);
			rs =prepstmt.executeQuery(); 
			Comment comment = null;
			while (rs.next()) {
				comment = new Comment(
				rs.getInt(1),
				rs.getInt(2),
				rs.getInt(3),
				rs.getString(4),
				rs.getTimestamp(5),
				rs.getTimestamp(6)
				);
				commentByUserIdList.add(comment);
			}

		} catch (SQLException e) {

			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException ignore) {
				}
			}
			if (prepstmt != null) {
				try {
					prepstmt.close();
				} catch (SQLException ignore) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException ignore) {
				}
			}
		}

		return commentByUserIdList;
	}
	/*
	 * add one comment
	 * */
	public boolean addOneComment(Comment comment){
		
		Connection conn = null;
		String sql = "INSERT INTO comments (userid, postid, comment) values(?,?,?)";
	    PreparedStatement pstmt = null;
	    
	    try {
			 conn = DBConnection.getInstance().getConnection();
			 pstmt = (PreparedStatement)conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			 pstmt.setInt(1, comment.getUserId());
			 pstmt.setInt(2, comment.getPostId());
			 pstmt.setString(3, comment.getCommentBody());
			 pstmt.executeUpdate();
			 ResultSet rs = pstmt.getGeneratedKeys();
					
			 if(rs.next())
				 comment.setCommentId(rs.getInt(1)); 
			 return true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
				
				if (pstmt != null)
					pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return false;
		
	}
	
	/*
	 * Delete one comment by User id
	 * */
public boolean deleteCommentByUserID(int userId, int comId){
		
		Connection conn = null;
		String sql = "DELETE FROM comments where userid = ? and commentid = ?";
	    PreparedStatement pstmt = null;
	    ResultSet rs = null;
	    
	    try {
			 conn = DBConnection.getInstance().getConnection();
			 pstmt = (PreparedStatement)conn.prepareStatement(sql);
			 pstmt.setInt(1, userId);
			 pstmt.setInt(2, comId);

			 return pstmt.executeUpdate() > 0;
			 
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
				
				if (pstmt != null)
					pstmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return false;
		
	}

}
