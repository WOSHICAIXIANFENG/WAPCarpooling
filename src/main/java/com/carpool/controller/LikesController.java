/**
 * 
 */
package com.carpool.controller;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.carpool.model.User;
import com.carpool.service.LikeService;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Diana Yamaletdinova
 *
 * Apr 25, 2017
 */
@WebServlet("/LikesController")
public class LikesController extends HttpServlet implements Servlet {

	private static final long serialVersionUID = 1L;

	private LikeService service;

	@Override
	public void init() throws ServletException {
		service = LikeService.getInstance();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		PrintWriter out = response.getWriter();
		String id = request.getParameter("postIdforLikes");
		int postId = Integer.parseInt(id);
		
		User user =(User) request.getSession().getAttribute("LoginUser");
		
		ObjectMapper mapper = new ObjectMapper();
		int likesCount = service.updateLikeTable(user.getUserid(), postId);
		String jsonStr = mapper.writeValueAsString(likesCount);
		out.print(jsonStr);
	}
}
