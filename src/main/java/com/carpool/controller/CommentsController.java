/**
 * 
 */
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
import javax.servlet.http.HttpSession;

import com.carpool.model.Comment;
import com.carpool.model.User;
import com.carpool.service.CommentService;
import com.carpool.util.ResultBean;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Diana Yamaletdinova
 *
 *         Apr 24, 2017
 */
@WebServlet("/CommentsController")
public class CommentsController extends HttpServlet implements Servlet {

	private static final long serialVersionUID = 1L;
	private CommentService service;

	@Override
	public void init() throws ServletException {
		service = CommentService.getInstance();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		PrintWriter out = response.getWriter();
		String id = request.getParameter("postId");
		int postId = Integer.parseInt(id);

		ObjectMapper mapper = new ObjectMapper();
		List<Comment> commentsList = new ArrayList<>();
		commentsList = service.getAllComments();
		//commentsList = service.getCommentsByPostId(postId);
		String jsonStr = mapper.writeValueAsString(commentsList);
		out.print(jsonStr);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {


		String action = request.getParameter("action");
		if ("addComment".equals(action)) {
			PrintWriter out = response.getWriter();
			ObjectMapper mapper = new ObjectMapper();
			
			User user =(User) request.getSession().getAttribute("LoginUser");
			String comBody = request.getParameter("comBody");
			String pId = request.getParameter("postId");
			int postId = Integer.parseInt(pId);
			
			ResultBean result = service.addOneComment(user.getUserid(), postId, comBody);
			if (result.isSuccess()) { 
				String jsonStr = mapper.writeValueAsString(result.getObj());
				out.print(jsonStr);
			} else {
				System.out.println("________________ERROR________________");
			}
		}else if ("deleteComment".equals(action)){
			User user =(User) request.getSession().getAttribute("LoginUser");
			String cId = request.getParameter("comId");
			int comId = Integer.parseInt(cId);
			ResultBean result = service.deleteCommentByUserId(user.getUserid(), comId);
			if (result.isSuccess()) {
				System.out.println("_______________Deleted successfully______________");
			} else {
				System.out.println("________________ERROR when deleting________________");
			}
		}

	}

}
