package com.carpool.controller;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.carpool.model.User;
import com.carpool.service.UserService;
import com.carpool.util.ResultBean;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/LoginServlet")
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LoginServlet() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.sendRedirect("index.jsp");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String jspage = request.getParameter("hidden");
		HttpSession session = request.getSession();
		if(jspage.equals("index")){ // change in later --- use login action directly
			// logic logic 
			//get email and password from db and check if matched
			String username=request.getParameter("email");
			String password=request.getParameter("password");
			String rememberMe = request.getParameter("rememberMe");
			
			ResultBean result = UserService.getInstance().doLogin(username, password);
			
			System.out.println("Samuel Test login result = " + result);
			if (result.isSuccess()) {
				if ("on".equals(rememberMe)) {
					Cookie cookie = new Cookie("user", username);
					cookie.setMaxAge(30 * 24 * 60 * 60);
					response.addCookie(cookie);
				} else {
					// clear cookie
					Cookie cookie = new Cookie("user", "");
					cookie.setMaxAge(0);
					response.addCookie(cookie);
				}
				
				// save current user into Session
				session.setAttribute("LoginUser", (User)(result.getObj()));
				
				//check if the specified user give the same password then
				request.getRequestDispatcher("mainpagenew.jsp").forward(request, response);
			} else {
				request.setAttribute("error_msg", result.getMsg()); 
				//response.sendRedirect("index.jsp");
				request.getRequestDispatcher("index.jsp").forward(request, response);
			}
		} else{
			// comes from different page so redirect
			response.sendRedirect("index.jsp");
		}
	}

}
