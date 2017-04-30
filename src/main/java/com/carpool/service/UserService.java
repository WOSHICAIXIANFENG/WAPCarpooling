package com.carpool.service;

import java.util.List;

import com.carpool.dao.UserDao;
import com.carpool.model.User;
import com.carpool.util.ResultBean;

/**
 * Service class for Users related bussiness logic
 * @author Xianfeng Cai
 */
public class UserService {
	private UserDao dao;
	
	private UserService() {
		dao = new UserDao();
	}
	
	private static volatile UserService instance;
	
	public static synchronized UserService getInstance() {
		if (instance == null) {
			synchronized(UserService.class) {
				if (instance == null) {
					instance = new UserService();
				}
			}
		}
		
		return instance;
	}
	
	
	/**
	 * Do login logic
	 * @param email
	 * @param password
	 * @return
	 */
	public ResultBean doLogin(String email, String password) {
		User user = dao.queryUserByEmail(email);
		if (user != null) {
			if (password.equals(user.getPassword())) {
				ResultBean bean = new ResultBean(true);
				bean.setObj(user);
				return bean;
			}
		}
		
		return new ResultBean(false, "Email or password is incorrect!");
	}
	
	/**
	 * Check if this user exists in our database
	 * @param username
	 * @param password
	 * @return
	 */
	public boolean isUserExists(String username, String password) {
		List<User> users = dao.queryUserByName(username);
		long count = users.stream().filter(a -> password.equals(a.getPassword())).count();
		if (count > 0) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Do sing up logic
	 * @param fullname
	 * @param password
	 * @param email
	 * @param gender
	 * @param state
	 * @param city
	 * @param street
	 * @param zipCode
	 * @param birthyear
	 * @return
	 */
	public ResultBean doSignUp(String fullname, String password,
			String email, String gender, String state, String city,
			String street, String zipCode, String birthyear ) {
		// check if this username already exists
		List<User> users = dao.queryUserByName(fullname);
		if (users.isEmpty()) {
			User user = new User();
			user.setEmail(email);
			user.setFullname(fullname);
			user.setCity(city);
			user.setState(state);
			user.setStreet(street);
			user.setPassword(password);
			user.setGender("M".equals(gender) ? true : false);
			user.setBirthyear(Integer.parseInt(birthyear));
			user.setZipcode(Integer.parseInt(zipCode));
			
			boolean result = dao.insertOneUser(user);
			if (result) {
				ResultBean bean = new ResultBean(true, "Sign Up Successfully!");
				bean.setObj(dao.queryUserByEmail(email));
				return bean;
			} else {
				return new ResultBean(false, "Sign Up fail! Please try again later");
			}
		} else {
			return new ResultBean(false, "Fullname already exists in our database!");
		}
	}
	
	
	/**
	 * Update Profile
	 * @param userId
	 * @param fullname
	 * @param email
	 * @param gender
	 * @param state
	 * @param city
	 * @param street
	 * @param zipCode
	 * @param birthyear
	 * @return ResultBean
	 */
	public ResultBean doProfileEdit(String userid, String fullname, String email, String gender, 
			String state, String city,
			String street, String zipCode, String birthyear ) {
		
		User user = new User();
		user.setUserid(Integer.parseInt(userid));
		user.setEmail(email);
		user.setFullname(fullname);
		user.setCity(city);
		user.setState(state);
		user.setStreet(street);
		user.setGender("M".equals(gender) ? true : false);
		user.setBirthyear(Integer.parseInt(birthyear));
		user.setZipcode(Integer.parseInt(zipCode));
		
		boolean result = dao.updateOneUser(user);
		if (result) {
			ResultBean bean = new ResultBean(true);
			bean.setObj(user);
			return bean;
		} else {
			return new ResultBean(false, "You update profile fail!");
		}
	}
}
