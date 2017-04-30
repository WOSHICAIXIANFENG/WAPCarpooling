package com.carpool.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.carpool.model.Post;
import com.carpool.model.User;
import com.carpool.service.PostService;
import com.carpool.util.ResultBean;
import com.fasterxml.jackson.databind.ObjectMapper;

@WebServlet("/PostsController")
public class PostsController extends HttpServlet implements Servlet {

	private static final long serialVersionUID = 1L;
	PostService service;

	@Override

	public void init() throws ServletException {
		service = PostService.getInstance();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String action = request.getParameter("action");
		
		PrintWriter out = response.getWriter();
		ObjectMapper mapper = new ObjectMapper();	
		List<Post> postsList = null;
		
		if ("querybytype".equals(action)) {
			String type = request.getParameter("type");
			postsList = service.getAllPostsByType(Integer.parseInt(type));		
		} else {
			postsList = service.getAllPosts();	
		}
		
		String jsonStr = mapper.writeValueAsString(postsList);
		out.print(jsonStr);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		String action = request.getParameter("action");
		User user =(User) request.getSession().getAttribute("LoginUser");
		if ("addPost".equals(action)) {
			
			String t = request.getParameter("rideTypeSel");
			int type = Integer.parseInt(t);
			String pickUpLocat = request.getParameter("pickUpLocat");
			String destinatLocat = request.getParameter("destinatLocat");
			
			ResultBean result = service.addOnePost(user.getUserid(), pickUpLocat, destinatLocat, type);
			//request.getRequestDispatcher("/mainpagenew.jsp").forward(request,response);
			if (result.isSuccess()) {
				System.out.println("_______________post added______________");
			} else {
				System.out.println("________________error when eding post________________");
			}
		}else if ("deletePost".equals(action)) {
			String id = request.getParameter("currentPostId");
			int currentPostId = Integer.parseInt(id);
			
			ResultBean result = service.deleteOnePostByPost(currentPostId);
			if (result.isSuccess()) {
				System.out.println("_______________post deleted______________");
			} else {
				System.out.println("________________error when deliting post________________");
			}
		}
	}

}
