<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<head>
<title>CarpoolingApp</title>
<link rel="stylesheet" type="text/css" href="headerfooter.css">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
<link rel="stylesheet" href="jquery-ui.css">
<link rel="stylesheet" href="tabsStyle.css">
<script src="jquery-1.11.1.js"></script>
<script src="jquery-ui.js"></script>
<script>
	
	$(function() {
		 $( "#tabs" ).tabs();
		 
		$('#posts').html('');
		$.ajax({
			'url' : 'PostsController',
			'type' : 'GET',
			'success' : function(data) {
				displayPostsInfo(JSON.parse(data));
			},
			'error' : ajaxFailure
		});
	});
	function ajaxFailure(xhr, status, exception) {
		console.log(xhr, status, exception);
	}

	function displayPostsInfo(posts) {

		for (var i = 0; i < posts.length; i++) {
			//current body
			var curBody = posts[i];
			//current post id that accosiated with that body
			var curPostId = posts[i].postId;
			//current post Type that accosiated with that body
			var curPostType = posts[i].postType;
			//current user id that accosiated with that body
			var userId = posts[i].userId;
			console.log(posts);
			var btn = document.createElement("input");
			btn.className = 'btn btn-info btn-xs';
			btn.setAttribute("id", curPostId);
			btn.setAttribute('type', 'button');
			btn.setAttribute('value', 'Show Comments');
			//$(btn).css('height','15px').css('width',' 70px');
			$(btn).css('font-size', '10px');
			
			var likeBtnId = 'l'+curPostId;
			var btnLike = document.createElement("input");
			btnLike.className = 'btn btn-primary btn-xs';
			btnLike.setAttribute("id", likeBtnId);
			btnLike.setAttribute("data-id", curPostId);
			btnLike.setAttribute('type', 'button');
			btnLike.setAttribute('value', 'Like');
			//$(btnLike).css('height','15px').css('width',' 70px');
			$(btnLike).css('font-size', '10px');


			//1 = need a ride=>"offer"
			if (curPostType === 1) {
				$('#offer').append("<br/>");
				$('#offer').append(curBody.postBody);
				$('#offer').append("<br/>");
				$('#offer').append(btn);
				$('#offer').append("&nbsp;&nbsp;");
				$('#offer').append(btnLike);
				$('#offer')
						.append('</br>')
						.append(
								'<span id = "c'+curPostId+'" style="color:blue;"></span>')
				$('#offer').append("<br/><hr>");
			} else if (curPostType === 2) { //2=offer a ride=>"request"
				$('#request').append("<br/>");
				$('#request').append(curBody.postBody);
				$('#request').append("<br/>");
				$('#request').append(btn);
				$('#request').append("&nbsp;&nbsp;");
				$('#request').append(btnLike);
				$('#request')
						.append('</br>')
						.append(
								'<span id = "c'+curPostId+'" style="color:blue;"></span>')
				$('#request').append("<br/><hr>");
			}

			var btnId = "\"#" + curPostId + "\"";

			//pass the post id to retreive relevant comments
			btn.onclick = function() {
				commentAjaxCall($(this).attr('id'));
			};

			//pass the post id to update likes
			btnLike.onclick = function() {
				likesAjaxCall($(this).data('id'));
			};
		}
	}

	function commentAjaxCall(postId) {

		$.ajax({
			'url' : "CommentsController",
			'data' : {
				"postId" : postId
			},
			'type' : 'GET',
			'success' : function(data) {
				displayCommentsInfo(JSON.parse(data), postId);
			},
			'error' : ajaxFailure
		});

	}

	function likesAjaxCall(postId) {
		$.ajax({
			'url' : "LikesController",
			'data' : {

				"postIdforLikes" : postId,
			},
			'type' : 'GET',
			'success' : function(data) {
				console.log(data);
				//displayLikes(data);

			},
			'error' : ajaxFailure
		});

	}

	function displayLikes(likes) {

	}

	function displayCommentsInfo(comments, postId) {
		var text = $('#' + postId).val();
		if (text == 'Hide Comments') {
			$('#' + postId).val('Show Comments');
			$('#c' + postId).slideUp();
			return false;
		}

		$('#c' + postId).html('').show();

		for (var i = 0; i < comments.length; i++) {
			var curPostId = comments[i].postId;
			//console.log(curPostId);
			var curCom = comments[i].commentBody;
			console.log(curCom);
			var nocom = "no comments";
			if (!curCom) {
				$('#c' + postId).append("</br>").append(nocom).append("<br/>");
			} else {

				if (curPostId == postId)
					$('#c' + postId).append("</br>").append(curCom).append(
							"<br/>");
			}

		}

		$('#c' + postId).show('slow');

		if ($('#' + postId).val() == 'Show Comments') {
			$('#' + postId).val('Hide Comments');
		} else {
			$('#' + postId).val('Show Comments');
		}
	}
</script>
</head>
<body>
	<div id="header">
		<div class="container">
			<h1>Product Company</h1>
		</div>
		<div id="container">
			<img
				src="http://d152j5tfobgaot.cloudfront.net/wp-content/uploads/2016/08/125-fall-in-love.png">
		</div>