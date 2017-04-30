package com.carpool.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.carpool.model.User;

import com.carpool.service.UserService;
import com.carpool.util.ResultBean;

/**
 * Controller to handle user Profile info
 * @author Xianfeng Cai
 */
@WebServlet(
		value = {"/ProfileController.do"},
		name = "MProfileController"
		)
public class ProfileController extends HttpServlet {
	private static final long serialVersionUID = 9172422423626099158L;
	private static final String ADD_USER_URL = "/signupjsp.jsp";
	private static final String EDIT_USER_URL = "/profile.jsp";
	private static final String HOMEPAGE_URL = "/mainpagenew.jsp";
	
	UserService us = null;

	@Override
	public void init() throws ServletException {
		super.init();
		us = UserService.getInstance();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		super.doGet(req, resp);
		
		// bind userId
		//req.setAttribute("LoginUser", );
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String action = req.getParameter("action");
		if ("edit".equals(action)) {		
			String userId = req.getParameter("userId");
			
			String fullname = req.getParameter("fullname");
			String gender= req.getParameter("gender");
			String state= req.getParameter("state");
			String city= req.getParameter("city");
			String street= req.getParameter("street");
			String zipcode= req.getParameter("zipcode");
			String birthyear= req.getParameter("birthyear");
			String email = req.getParameter("email");
			
			ResultBean result = us.doProfileEdit(userId, fullname, email, gender, state, city, street, zipcode, birthyear);
			if (result.isSuccess()) {
				// redirection to homepage
				req.setAttribute("error_msg", "");
				req.getSession().setAttribute("LoginUser", (User)(result.getObj()));
				
				req.getRequestDispatcher(HOMEPAGE_URL).forward(req, resp);
			} else {
				req.setAttribute("error_msg", result.getMsg());
				req.getRequestDispatcher(EDIT_USER_URL).forward(req, resp);
			}
		} else if ("create".equals(action)) {
			String email = req.getParameter("email");
			String fullname = req.getParameter("fullname");
			String gender = req.getParameter("gender");
			String state = req.getParameter("state");
			String city = req.getParameter("city");
			String street = req.getParameter("street");
			String zipcode = req.getParameter("zipcode");
			String birthyear = req.getParameter("birthyear");
			String password = req.getParameter("password");
			
			ResultBean result = UserService.getInstance().doSignUp(fullname, password, email, gender, state, city, street, zipcode, birthyear);
			//System.out.println("Test output result = " + result);
			
			if (result.isSuccess()) {
				req.setAttribute("error_msg", "");
				// save current user into Session
				if (result.getObj() != null)
					req.getSession().setAttribute("LoginUser", (User)(result.getObj()));
				
				// enter into home page
				req.getRequestDispatcher(HOMEPAGE_URL).forward(req, resp);
			} else {
				req.setAttribute("error_msg", result.getMsg());
				// forward to the 
				req.getRequestDispatcher(ADD_USER_URL).forward(req, resp);
			}
		}
	}
	
}
