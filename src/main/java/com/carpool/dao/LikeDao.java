package com.carpool.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.carpool.jdbc.DBConnection;
import com.carpool.model.Like;
import com.mysql.cj.jdbc.PreparedStatement;

/**
 * CRUD for Model Like --- db table likes.
 * 
 * @author Xianfeng Cai
 *
 */
public class LikeDao {

	public LikeDao() {

	}

	public int getAllLikesByPostId(int postId) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String sql = "select count(*) from likes where postid = ?";
		PreparedStatement prepstmt = null;
		int likesCount = 0;
		try {
			conn = DBConnection.getInstance().getConnection();
			prepstmt = (PreparedStatement)conn.prepareStatement(sql);
			prepstmt.setInt(1, postId);
			rs = prepstmt.executeQuery();
			while (rs.next()) {
				likesCount = rs.getInt(1);
			}
			/*while (rs.next()) {
				Like like = new Like(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getTimestamp(4), rs.getTimestamp(5));
				likeList.add(like);
			}*/
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
System.out.println("likesCount " + likesCount);
		return likesCount;
	}

	/**
	 * Insert likes into DB
	 */
	public int insertLike(int userId, int postId) {
		Connection conn = null;
		String sql = "INSERT INTO likes (userid, postid) values(?,?)";
		PreparedStatement stmt = null;

		try {
			conn = DBConnection.getInstance().getConnection();
			stmt = (PreparedStatement) conn.prepareStatement(sql);

			stmt.setInt(1, userId);
			stmt.setInt(2, postId);

			 stmt.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();

				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		int numLikesForThisPost = getAllLikesByPostId(postId);
		return numLikesForThisPost;
	}

	/**
	 * Insert likes into DB
	 */
	public int insertLikeRows(int userId, int postId) {
		//if (insertLike(userId, postId)) {

			int numRows = 0;
			Connection conn = null;
			String sql = "SELECT * FROM likes WHERE postid = ?";
			ResultSet rs = null;
			PreparedStatement stmt = null;

			try {
				conn = DBConnection.getInstance().getConnection();
				stmt = (PreparedStatement) conn.prepareStatement(sql);
				stmt.setInt(1, postId);
				rs = stmt.executeQuery();

				while (rs.next()) {
					numRows = rs.getInt(1);
					System.out.println("numberOfRows= " + numRows);
				}

			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					if (conn != null)
						conn.close();

					if (stmt != null)
						stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			return numRows;

		//}
		//return 0;
	}
}
