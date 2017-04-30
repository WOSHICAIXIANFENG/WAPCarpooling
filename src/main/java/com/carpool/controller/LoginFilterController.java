package com.carpool.controller;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Filter ---- avoid all stolen urls
 * 
 * Servlet Filter implementation class LoginFilterController
 */
@WebFilter(
		filterName="LoginFilterController",
		value={"/profile.jsp", "/navbar.jsp", "/mainpage.jsp", "/header.jsp", "/footer.jsp"},
		servletNames={"LoginServlet"})
public class LoginFilterController implements Filter {

    /**
     * Default constructor. 
     */
    public LoginFilterController() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Filter#destroy()
	 */
	public void destroy() {
		// TODO Auto-generated method stub
	}

	/**
	 * @see Filter#doFilter(ServletRequest, ServletResponse, FilterChain)
	 */
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		 	HttpServletRequest req = (HttpServletRequest) request;
	        HttpServletResponse res = (HttpServletResponse) response;
	        HttpSession session = req.getSession(false);
	        String loginURI = req.getContextPath() + "/LoginServlet";

	        boolean loggedIn = session != null && session.getAttribute("LoginUser") != null;
	        boolean loginRequest = req.getRequestURI().equals(loginURI);
	        
	        if (loggedIn || loginRequest) {
	        	chain.doFilter(request, response);
	           } else {
	            res.sendRedirect(loginURI);
	        }
		
	}

	/**
	 * @see Filter#init(FilterConfig)
	 */
	public void init(FilterConfig fConfig) throws ServletException {
		// TODO Auto-generated method stub
	}

}
