package com.carpool.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.carpool.jdbc.DBConnection;
import com.carpool.model.User;
import com.mysql.cj.jdbc.PreparedStatement;

/**
 * CRUD for USER Entity
 * @author Xianfeng Cai
 */
public class UserDao {
	
	public UserDao() {
		
	}
	
	
	/**
	 * Query user by email
	 * @param fullname
	 * @param password
	 * @return List<User>
	 */
	public User queryUserByEmail(String email) {
		// email should be unique for everyone
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = "SELECT * FROM USERS WHERE email = ?";
		ResultSet rs = null;
		try {
			conn = DBConnection.getInstance().getConnection();
			pstmt = (PreparedStatement)conn.prepareStatement(sql);
			pstmt.setString(1, email);
			rs = pstmt.executeQuery();
			
			User user = null;
			if (rs.next()) {
				user = new User(
						rs.getInt(1),
						rs.getString(2),
						rs.getInt(3) == 1 ? true : false,
						rs.getString(4),
						rs.getString(5),
						rs.getString(6),
						rs.getInt(7),
						rs.getInt(8),
						rs.getString(9),
						rs.getString(10)
						);
				return user;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
				
				if (pstmt != null)
					pstmt.close();
				
				if (rs != null) 
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	/**
	 * Query user by fullname
	 * @param fullname
	 * @param password
	 * @return List<User>
	 */
	public List<User> queryUserByName(String fullname) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = "SELECT * FROM USERS WHERE fullname = ?";
		ResultSet rs = null;
		List<User> usersList = new ArrayList<>();
		try {
			conn = DBConnection.getInstance().getConnection();
			pstmt = (PreparedStatement)conn.prepareStatement(sql);
			pstmt.setString(1, fullname);
			rs = pstmt.executeQuery();
			
			User user = null;
			while (rs.next()) {
				user = new User(
						rs.getInt(1),
						rs.getString(2),
						rs.getInt(3) == 1 ? true : false,
						rs.getString(4),
						rs.getString(5),
						rs.getString(6),
						rs.getInt(7),
						rs.getInt(8),
						rs.getString(9),
						rs.getString(10)
						);
				usersList.add(user);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
				
				if (pstmt != null)
					pstmt.close();
				
				if (rs != null) 
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return usersList;
	}
	
	/**
	 * Query user by userId
	 * @param userId --- unique id for user entity
	 * @return User
	 */
	public User getUserById(int userId) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = "SELECT * FROM USERS WHERE userid = ?";
		ResultSet rs = null;
		try {
			conn = DBConnection.getInstance().getConnection();
			pstmt = (PreparedStatement)conn.prepareStatement(sql);
			pstmt.setInt(1, userId);
			rs = pstmt.executeQuery();
			if (rs.next()) { // todo : need test 
				User user = new User(
						rs.getInt(1),
						rs.getString(2),
						rs.getInt(3) == 1 ? true : false,
						rs.getString(4),
						rs.getString(5),
						rs.getString(6),
						rs.getInt(7),
						rs.getInt(8),
						rs.getString(9),
						rs.getString(10)
						);
				return user;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
				
				if (pstmt != null)
					pstmt.close();
				
				if (rs != null) 
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * Insert one User into DB
	 * @param user
	 * @return boolean true --- insert successfully; false ---- insert failly
	 */
	public boolean insertOneUser(User user) {
		Connection conn = null;
		String sql = "INSERT INTO USERS (fullname, gender, state, city, street, zipcode, birthyear, email, password) values(?,?,?,?,?,?,?,?,?)";
	    PreparedStatement pstmt = null;
	    
		try {
			 conn = DBConnection.getInstance().getConnection();
			 pstmt = (PreparedStatement)conn.prepareStatement(sql);
			 pstmt.setString(1, user.getFullname());
			 pstmt.setInt(2, user.isGender() ? 1 : 0);
			 pstmt.setString(3, user.getState());
			 pstmt.setString(4, user.getCity());
			 pstmt.setString(5, user.getStreet());
			 pstmt.setInt(6, user.getZipcode());
			 pstmt.setInt(7, user.getBirthyear());
			 pstmt.setString(8, user.getEmail());
			 pstmt.setString(9, user.getPassword());
			 
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
	
	/**
	 * Update one user
	 * @param user
	 * @return boolean if user been updated successfully
	 */
	public boolean updateOneUser(User user) {
		Connection conn = null;
		String sql = "UPDATE USERS SET"
				+ " fullname = ?, gender = ?, state = ?, city = ?, street = ?, zipcode = ?, birthyear = ?, email = ?"
				+ " WHERE userid = ? ";
	    PreparedStatement pstmt = null;
	    
		try {
			 conn = DBConnection.getInstance().getConnection();
			 pstmt = (PreparedStatement)conn.prepareStatement(sql);
			 pstmt.setString(1, user.getFullname());
			 pstmt.setInt(2, user.isGender() ? 1 : 0);
			 pstmt.setString(3, user.getState());
			 pstmt.setString(4, user.getCity());
			 pstmt.setString(5, user.getStreet());
			 pstmt.setInt(6, user.getZipcode());
			 pstmt.setInt(7, user.getBirthyear());
			 pstmt.setString(8, user.getEmail());
			 
			 pstmt.setInt(9, user.getUserid());
			 
			 int i = pstmt.executeUpdate();
			 if (i > 0) {
				 return true;
			 } else {
				 // log error message
			 }
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
	
	/**
	 * Remove one User from DB
	 * @param userId
	 * @return boolean
	 */
	public boolean removeOneUser(int userId) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = "DELETE FROM TABLE USERS WHERE userid = ?";
		try {
			 conn = DBConnection.getInstance().getConnection();
			 pstmt = (PreparedStatement)conn.prepareStatement(sql);
			 pstmt.setInt(1, userId);
			 
			 int i = pstmt.executeUpdate();
			 if (i > 0) {
				 return true;
			 } else {
				 // log error message
			 }
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
	
//	/**
//	 * Generate one userId ---- keep userId is unique
//	 * @return
//	 */
//	private int generateUserId(Connection conn) throws SQLException {
//		// query the database find the max userId, then ++
//		String sql = "SELECT MAX(userid) AS LargestUserId FROM USERS";
//		PreparedStatement pstmt = (PreparedStatement)conn.prepareStatement(sql);
//		ResultSet rs = pstmt.executeQuery();
//		try {
//			if (rs == null) {
//				return 1;
//			} else {
//				if(rs.next()) 
//					return rs.getInt(1) + 1;
//				else 
//					return 1;
//			}
//		} finally {	
//			if (pstmt != null)
//				pstmt.close();
//			
//			if (rs != null)
//				rs.close();
//		}
//	}
}
