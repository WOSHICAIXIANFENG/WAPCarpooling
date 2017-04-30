<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<meta charset="utf-8">
<title>CarPooling</title>
<link rel="stylesheet" type="text/css" href="style.css">
<script	src="https://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
<script type="text/javascript"src="http://ajax.googleapis.com/ajax/libs/jquery/1.4.2/jquery.min.js"></script>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">

<script src="resources/js/jquery.transit.min.js" type="text/javascript"></script>
<script src="resources/js/socket_client.js" type="text/javascript"></script>

<link rel="stylesheet" href="jquery-ui.css">
<link rel="stylesheet" href="tabsStyle.css">
<link
	href="http://getbootstrap.com/examples/sticky-footer/sticky-footer.css"
	rel="stylesheet">
<script src="jquery-1.11.1.js"></script>
<script src="jquery-ui.js"></script>
<script src="scrolling.js"></script>
<script type="text/javascript">
	var postsList;
	var currPostPosition;
	var currentUser = ${LoginUser.userid};
	var currentCommentId;
	var numOfLikes;


    
	$(function() {
		jQuery("#dialog").hide();
		jQuery("#dialogRide").hide();

		let url = "http://www.wunderground.com/cgi-bin/findweather/getForecast?query=";
	    $("#check_weather").click(function(){
	    	//window.location=url + $("#destination").val();
	    	$(location).attr("href", url + $("#destination").val());
	    });
        
	    /*
	    * function to display the pop up dialog for offer/ask for a ride
	    */
		$("a#To")
				.click(
						function(e) {
							//e.preventDefault();
							$("#dialogRide")
									.dialog(
											{
												height : 380,
												width : 450,
												modal : true,
												position : {
													my : 'top',
													at : 'top+290'
												},
												buttons : {
													"Cancel" : function() {
														$(this).dialog("close");
													},
													"Submit" : function() {
														var destinatLocat = $(
																'#sel3').val();
														var pickUpLocat = $(
																'#sel2').val();
														var rideTypeSel = $(
																'#sel1').val();
														//console.log(destinatLocat);
														//console.log(pickUpLocat);
														//console.log(rideTypeSel);
														addNewPostAjaxCall(
																rideTypeSel,
																pickUpLocat,
																destinatLocat);

														let postBody = "";
														if ($("#sel1").val() == "2") {
															postBody = "I am offering a ride from "
																	+ pickUpLocat
																	+ " to "
																	+ destinatLocat;
														} else {
															postBody = "I need a ride from "
																	+ pickUpLocat
																	+ " to "
																	+ destinatLocat;
														}

														var post = {
															"userid" : parseInt($(
																	"#myLink")
																	.attr(
																			"myAttr")),
															"post" : postBody,
															"posttype" : rideTypeSel
														};
														let jsonStr = JSON
																.stringify(post);
														// send this new Post to other clients
														sendPostToServer(jsonStr);
														$(this).dialog('close');
													},
												}
											});
						});
	    
	    /*
	    * endless scroll function for two tabs
	    */
		$('#offeride').endlessScroll({
			inflowPixels : 300,
			callback : function() {
				addOffers();
			}
		});

		$('#getride').endlessScroll({
			inflowPixels : 300,
			callback : function() {
				addRides();
			}
		});

		/*
		*AJAX call to retreive the data for the posts tabs
		*/
		$('#posts').html('');
		$.ajax({
			'url' : 'PostsController',
			'type' : 'GET',
			'success' : function(data) {
				postsList = JSON.parse(data);
				displayPostsInfo(postsList, '#offer', 2, 0);
				displayPostsInfo(postsList, '#request', 1, 0);
			},
			'error' : ajaxFailure
		});
	});
	
	/*
	* Failure method
	*/
	function ajaxFailure(xhr, status, exception) {
		console.log(xhr, status, exception);
	}

	/*
	* method to add posts to span offer
	*/
	function addOffers() {
		displayPostsInfo(postsList, '#offer', 2, currPostPosition);
	}

	/*
	* method to add posts to span request
	*/
	function addRides() {
		displayPostsInfo(postsList, '#request', 1, currPostPosition);
	}

	/*
	* generic method to add posts to spans
	*/
	function displayPostsInfo(posts, elementId, carpoolType, index) {
		console.log(posts);
		var postTypeCounter = 0;
		var i = index;
		var curPostId = 0;
		for (; i < posts.length; i++) {

			if (postTypeCounter >= 3) {
				currPostPosition = i;
				break;
			}

			//current body
			var curBody = posts[i];
			//current post id that accosiated with that body
			curPostId = posts[i].postId;
			//current post Type that accosiated with that body
			var curPostType = posts[i].postType;
			
			//current user id that accosiated with that body
			var userId = posts[i].userId;
			var fName = posts[i].fullname;
			
			//DELETE POST BTN
			var delPostBtnId = curPostId + "del";
			var deletePostBtn = document.createElement("input");
			deletePostBtn.className = 'btn btn-info btn-xs';
			deletePostBtn.setAttribute("id", delPostBtnId);
			deletePostBtn.setAttribute('type', 'button');
			deletePostBtn.setAttribute("data-del", curPostId);
			deletePostBtn.setAttribute('value', 'Delete');
			//$(btn).css('height','15px').css('width',' 70px');
			$(deletePostBtn).css('font-size', '12px');
			
			//SHOW COMMENT BTN
			var showCommBtn = document.createElement("input");
			showCommBtn.className = 'btn btn-primary btn-xs';
			showCommBtn.setAttribute("id", curPostId);
			showCommBtn.setAttribute('type', 'button');
			showCommBtn.setAttribute('value', 'Show Comments');
			//$(btn).css('height','15px').css('width',' 70px');
			$(showCommBtn).css('font-size', '12px');

			//ADD COMMENT BTN
			var addComBtnId = 'addC' + curPostId;
			var addCommBtn = document.createElement("input");
			addCommBtn.className = 'btn btn-primary btn-xs';
			addCommBtn.setAttribute('data-toggle', 'modal');
			addCommBtn.setAttribute('data-target', 'myModal');
			addCommBtn.setAttribute("id", addComBtnId);
			addCommBtn.setAttribute("data-val", curPostId);
			addCommBtn.setAttribute('type', 'button');
			addCommBtn.setAttribute('value', 'Add Comments');
			//$(btn).css('height','15px').css('width',' 70px');
			$(addCommBtn).css('font-size', '12px');

			//LIKE BTN
			var likeBtnId = 'l' + curPostId;
			var btnLike = document.createElement("input");
			btnLike.className = 'btn btn-primary btn-xs';
			btnLike.setAttribute("id", likeBtnId);
			btnLike.setAttribute("data-i", curPostId);
			btnLike.setAttribute('type', 'button');
			btnLike.setAttribute('value', 'Like');
			//$(btnLike).css('height','15px').css('width',' 70px');
			$(btnLike).css('font-size', '11px');
			
			
			
			var curPostType = posts[i].postType;
			//console.log(curBody.fullame);
			if (curPostType === carpoolType) {
				//console.log(fullName);
				//element.setAttribute("data-id", curPostId);
				//var spanPost = document.createElement("span");
				//spanPost.setAttribute("data-span", curPostId);
				//$(elementId).append("<br/>").append(fullName);
				
				/*$(elementId)
						.append('</br>')
						.append(
								'<span id = "fullname" style="color:blue;"></span>').append("<br/><hr>");*/
				$(elementId).append('<span id="name"></span>');
				//$("#name").append (curBody.fullname);
				$(elementId).append("User: ").append(curBody.userId).append("<br/>");
				$(elementId).attr("data-postdel", curPostId);
				$(elementId).append("<br/>").append(curBody.postBody);
				//$(elementId).append(curBody.postBody);
				
				if (curBody.userId === currentUser){
					$(elementId).append("&nbsp;&nbsp;").append(deletePostBtn).append("<br/>");
				}
				$(elementId).append("<br/>");
				$(elementId).append(showCommBtn).append("&nbsp;&nbsp;");
				//$(elementId).append("&nbsp;&nbsp;");
				$(elementId).append(addCommBtn).append("&nbsp;&nbsp;").append(btnLike);
				$(elementId).append('&nbsp;<span id="likes' + curPostId +'"></span>');
				//$(elementId).append(btnLike);
				$(elementId)
						.append('</br>')
						.append(
								'<span id = "c'+curPostId+'" style="color:blue;"></span>').append("<br/><hr>");

				postTypeCounter++;
			}

			var btnId = "\"#" + curPostId + "\"";

			//pass the post id to retreive relevant comments
			showCommBtn.onclick = function() {
				showCommentAjaxCall($(this).attr('id'));
			};

			//pass the post id to retreive relevant comments
			addCommBtn.onclick = function() {
				var postid = $(this).data('val');
				$("#dialog").dialog({
					height : 250,
					width : 450,
					modal : true,
					position : {
						my : 'top',
						at : 'top+290'
					},
					buttons : {
						"Cancel" : function() {
							$(this).dialog("close");
							$('#comBody').val("");
						},
						"Submit" : function() {
							addCommentAjaxCall(postid, $('#comBody').val());
							$('#comBody').val("");
							$(this).dialog('close');
						},
					}
				});

			}

			//pass the post id to update likes
			btnLike.onclick = function() {
				likesAjaxCall($(this).data('i'));
			};
			
			deletePostBtn.onclick = function(){
				deletePostAjaxCall($(this).data('del'));
				$($(this).data('del')).parent().remove();
			};
		}

		if (i >= posts.length) {
			currPostPosition = posts.length;
		}
	}

	/*
	*Ajax call to delete posts that belong to logged in user
	*/
	function deletePostAjaxCall(postId){
		$.ajax({
			'url' : "PostsController",
			'data' : {
				'action' : 'deletePost',
				"currentPostId" : postId
			},
			'type' : 'POST',
			'success' : function(data) {
				//remove post from the view
				//$('span[data-id='" + postId + "']').remove();
				$('[data-postdel="'+postId+'"]').remove();
				//$(this).remove();
				//$('#request').empty();
			},
			'error' : ajaxFailure
		});
	}
	
	/*
	* Ajax call to add new posts
	*/
	function addNewPostAjaxCall(rideTypeSel, pickUpLocat, destinatLocat) {
		//console.log(destinatLocat);
		//console.log(pickUpLocat);
		//console.log(rideTypeSel);
		$.ajax({
			'url' : "PostsController",
			'data' : {
				'action' : 'addPost',
				"rideTypeSel" : rideTypeSel,
				"pickUpLocat" : pickUpLocat,
				"destinatLocat" : destinatLocat
			},
			'type' : 'POST',
			'success' : function(data) {
			   // displayCommentsInfo(JSON.parse(data), postId);
				// refresh page
				refreshOneListViewByType(rideTypeSel);
			},
			'error' : ajaxFailure
		});
	}

	// refresh one ListView after I update the local database
	function refreshOneListViewByType(rideTypeSel, callbackFun) {
		// patical refresh page by its type ----- need to call ajax to query database
		// todo
		$.ajax({
			'url' : 'PostsController?action=querybytype&type=' + rideTypeSel,
			'type' : 'GET',
			'success' : function(data) {
				var myList = JSON.parse(data);
				//console.log(JSON.parse(data));
				if (rideTypeSel == "2") {
					$('#offer').empty();
					displayPostsInfo(myList, '#offer', 2, 0);
				} else if (rideTypeSel == "1") {
					$('#request').empty();
					displayPostsInfo(myList, '#request', 1, 0);
				}

				if (callbackFun)
					callbackFun();
			},
			'error' : ajaxFailure
		});
	}

	function addCommentAjaxCall(postId, comBody) {
		//console.log(postId);
		$.ajax({
			'url' : "CommentsController",
			'data' : {
				'action' : 'addComment',
				"postId" : postId,
				"comBody" : comBody
			},
			'type' : 'POST',
			'success' : function(data) {
				//console.log(data);
				addNewComment(postId, comBody, JSON.parse(data).commentId);
			},
			'error' : ajaxFailure
		});
	}

	function addNewComment(postId, comBody, CurComId) {
		$('#c' + postId).append("</br>").append(
				'<span id = "del'+ CurComId+'" style="color:red;"></span>');
		$('#del' + CurComId).append("</br>").append(comBody).append(
				"&nbsp;&nbsp;&nbsp;");

		var deleteComBtnId = 'btn' + postId;
		var deleteComBtn = document.createElement("input");
		deleteComBtn.className = 'btn btn-primary btn-xs';
		deleteComBtn.setAttribute("id", deleteComBtnId);
		deleteComBtn.setAttribute("data-val", postId);
		deleteComBtn.setAttribute('type', 'button');
		deleteComBtn.setAttribute('value', 'Delete');
		//$(btn).css('height','15px').css('width',' 70px');
		$(deleteComBtn).css('font-size', '11px');
		$('#del' + CurComId).append(deleteComBtn);

		//on click of the delete button ajax call pass the post id to update likes
		deleteComBtn.onclick = function() {
			deleteCommentAjaxCall(CurComId);

		};

	}

	function showCommentAjaxCall(postId) {

		$.ajax({
			'url' : "CommentsController",
			'data' : {

				"postId" : postId
			},
			'type' : 'GET',
			'success' : function(data) {
				//console.log(JSON.parse(data));
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
				displayLikesCounter(JSON.parse(data));
				
			},
			'error' : ajaxFailure
		});

	}

	function displayLikesCounter (numOfLikes){
		$('#likes' + curPostId).append(numOfLikes);
	}
	function displayCommentsInfo(comments, postId) {
		var text = $('#' + postId).val();
		if (text == 'Hide Comments') {
			$('#' + postId).val('Show Comments');
			$('#c' + postId).slideUp();
			return false;
		}

		$('#c' + postId).html('').show();
		var curUserId = 0;

		for (var i = 0; i < comments.length; i++) {
			//console.log(comments);
			var curPostId = comments[i].postId;

			//console.log(curUserId);
			//console.log(curPostId);
			var curCom = comments[i].commentBody;
			var CurComId = comments[i].commentId;
			//console.log(CurComId);
			currentCommentId = CurComId;

			if (curPostId == postId) {
				curUserId = comments[i].userId;

				$('#c' + postId)
						.append("</br>")
						.append(
								'<span id = "del'+ CurComId+'" style="color:red;"></span>');

				$('#del' + CurComId).append("</br>").append(curCom).append(
						"&nbsp;&nbsp;&nbsp;");
				if (curUserId === currentUser) {

					var deleteComBtnId = 'btn' + CurComId;
					var deleteComBtn = document.createElement("input");
					deleteComBtn.className = 'btn btn-primary btn-xs';
					deleteComBtn.setAttribute("id", deleteComBtnId);
					deleteComBtn.setAttribute("data-val", curPostId);
					deleteComBtn.setAttribute("data-id", CurComId);
					deleteComBtn.setAttribute('type', 'button');
					deleteComBtn.setAttribute('value', 'Delete');
					//$(btn).css('height','15px').css('width',' 70px');
					$(deleteComBtn).css('font-size', '11px');
					$('#del' + CurComId).append(deleteComBtn);
					currentCommentId = CurComId;
					//on click of the delete button ajax call pass the post id to update likes
					deleteComBtn.onclick = function() {
						deleteCommentAjaxCall($(this).data('id'));
					};
					
					$('#c' + postId).show('slow');
					if ($('#' + postId).val() == 'Show Comments') {
						$('#' + postId).val('Hide Comments');
					} else {
						$('#' + postId).val('Show Comments');
					}
				}				
			}
		}
	}
	
/*
 * Delete a comment
 */
	function deleteCommentAjaxCall(comId) {
		$.ajax({
			'url' : "CommentsController",
			'data' : {
				'action' : 'deleteComment',
				"comId" : comId,
			},
			'type' : 'POST',
			'success' : function(data) {
				$('#del' + comId).remove();
			},
			'error' : ajaxFailure
		});
	}

	function openPost(evt, postName) {
		// Declare all variables
		var i, tabcontent, tablinks;

		// Get all elements with class="tabcontent" and hide them
		tabcontent = document.getElementsByClassName("tabcontent");
		for (i = 0; i < tabcontent.length; i++) {
			tabcontent[i].style.display = "none";
		}

		// Get all elements with class="tablinks" and remove the class "active"
		tablinks = document.getElementsByClassName("tablinks");
		for (i = 0; i < tablinks.length; i++) {
			tablinks[i].className = tablinks[i].className
					.replace(" active", "");
		}

		// Show the current tab, and add an "active" class to the button that opened the tab
		document.getElementById(postName).style.display = "block";
		evt.currentTarget.className += " active";

		// add by Xianfeng
		// use ajax to call local database to load data by type. after page refresh, clear notification view
		let myType = 2;
		if (postName == 'offeride') { // type 2
			myType = 2;
		} else if (postName == 'getride') {
			myType = 1;
		}

		refreshOneListViewByType(myType, function() {
			clearNotificationView(myType);
		});
	}

</script>

</head>
<body>
	<header> <nav id="header-nav" class="navbar navbar-default">
	<div class="container"></div>
	<div class="navbar-header">
		<a href="index.html" class="pull-left visible-md visible-lg">
			<div id="logo-img" alt="Logo image"></div>
		</a>
		<div class="navbar-brand">
			<a href="index.html"><h1>MUM CarPooling</h1></a>
		</div>
	</div>
	</nav> </header>
	<div id="navdiv">
		<ul class="nav">

			<li><a href="#">About</a>
				<ul>
					<li><a href="#">Mission</a></li>
					<li><a href="#">History</a></li>
				</ul></li>
			<li><a href="geolocation.jsp">Geolocation</a>
				</li>
			<li><a href="#">Destination</a>
				<ul>
					<input type="text" id="destination" placeholder="City,State/Country" required />
					<li><a id='check_weather' title='Location Weather Forecast'>Click for weather forecast</a></li>
				</ul>
			</li>

			<li><a href="#">My Trips</a>
				<ul>
					<li><a href="#" id="To">Plan a Trip</a></li>
					<li><a href="#" id="">All my Rides</a></li>

				</ul></li>
		</ul>
		<div id="profile">
			<ul class="nav">
				<li><a id="myLink" myAttr="${LoginUser.userid}">${LoginUser.fullname}</a>
					<ul>
						<li><a href="profile.jsp">Profile</a></li>
						<li><a href="logout">Logout</a></li>
					</ul>
			</ul>
		</div>
	</div>

	<!-- start of post display tabs -->
	<div class="tab">
		<button class="tablinks" onclick="openPost(event,'offeride')">
			Offer Ride <span id="notify1" class="notification-counter"></span>
		</button>
		<button class="tablinks" onclick="openPost(event,'getride')">
			Ask For A Ride <span id="notify2" class="notification-counter"></span>
		</button>
	</div>

	<div id="offeride" class="tabcontent">
	
		<span id="offer"> </span></span> <br />
	</div>

	<div id="getride" class="tabcontent">
		<span id="request"> 
		
		</span>
	</div>
	<!-- End of post display tabs -->

	<br />

	<div id="dialog" title="Leave a Comment" class="dialog">


		<br>
		<div class="form-group">
			<textarea "
		id="comBody" rows="5" cols="39">
		</textarea>
		<br />
	</div>

	</div>

	<div id="dialogRide" title="Going somewhere?" class="dialog">

		<form id="selecttest">
			<label>Please select the type of the ride:</label>
			<div class="form-group">
				<label for="sel1">Select list:</label> 
				<select class="form-control"	id="sel1">
					<option value="2">Offer a ride</option>
					<option value="1">Request a ride</option>

				</select>
				 <label>Please select pick up location:</label> 
				<select	class="form-control" id="sel2">
					<option value="Des Moines">Des Moines</option>
					<option value="Fairfield">Fairfield</option>
					<option value="Ottumwa">Ottumwa</option>
					<option value="Vedic City">Vedic City</option>
					<option value="Ames">Ames</option>
					<option value="Ankeny">Ankeny</option>
				</select> 
				<label>Please select destination:</label> 
				<select	class="form-control" id="sel3">
					<option value="Des Moines">Des Moines</option>
					<option value="Fairfield">Fairfield</option>
					<option value="Ottumwa">Ottumwa</option>
					<option value="Vedic City">Vedic City</option>
					<option value="Ames">Ames</option>
					<option value="Ankeny">Ankeny</option>
				</select>
			</div>
		</form>
	</div>

	<div id="weatherform">
		<!-- weather widget start -->
		<div id="m-booked-bl-simple-week-vertical-54751">
			<div class="booked-wzs-160-275 weather-customize"
				style="background-color: #137AE9; width: 160px;" id="width1 ">
				<a target="_blank" class="booked-wzs-top-160-275"
					href="http://www.booked.net/"><img
					src="//s.bookcdn.com/images/letter/s5.gif"
					alt="http://www.booked.net/" />
					</a>
				<div class="booked-wzs-160-275_in">
					<div class="booked-wzs-160-275-data">
						<div class="booked-wzs-160-275-left-img wrz-01"></div>
						<div class="booked-wzs-160-275-right">
							<div class="booked-wzs-day-deck">
								<div class="booked-wzs-day-val">
									<div class="booked-wzs-day-number">
										<span class="plus">+</span>24
									</div>
									<div class="booked-wzs-day-dergee">
										<div class="booked-wzs-day-dergee-val">&deg;</div>
										<div class="booked-wzs-day-dergee-name">C</div>
									</div>
								</div>
								<div class="booked-wzs-day">
									<div class="booked-wzs-day-d">
										<span class="plus">+</span>25&deg;
									</div>
									<div class="booked-wzs-day-n">
										<span class="plus">+</span>16&deg;
									</div>
								</div>
							</div>
							<div class="booked-wzs-160-275-info">
								<div class="booked-wzs-160-275-city">Fairfield</div>
								<div class="booked-wzs-160-275-date">Monday, 24</div>
							</div>
						</div>
					</div>
					<a target="_blank"
						href="http://www.booked.net/weather/fairfield-3148"
						class="booked-wzs-bottom-160-275">
						<table cellpadding="0" cellspacing="0" class="booked-wzs-table-160">
							<tr>
								<td class="week-day"><span class="week-day-txt">Tuesday</span></td>
								<td class="week-day-ico"><div class="wrz-sml wrzs-06"></div></td>
								<td class="week-day-val"><span class="plus">+</span>25&deg;</td>
								<td class="week-day-val"><span class="plus">+</span>13&deg;</td>
							</tr>
							<tr>
								<td class="week-day"><span class="week-day-txt">Wednesday</span></td>
								<td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td>
								<td class="week-day-val"><span class="plus">+</span>27&deg;</td>
								<td class="week-day-val"><span class="plus">+</span>11&deg;</td>
							</tr>
							<tr>
								<td class="week-day"><span class="week-day-txt">Thursday</span></td>
								<td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td>
								<td class="week-day-val"><span class="plus">+</span>13&deg;</td>
								<td class="week-day-val"><span class="plus">+</span>5&deg;</td>
							</tr>
							<tr>
								<td class="week-day"><span class="week-day-txt">Friday</span></td>
								<td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td>
								<td class="week-day-val"><span class="plus">+</span>14&deg;</td>
								<td class="week-day-val"><span class="plus">+</span>7&deg;</td>
							</tr>
							<tr>
								<td class="week-day"><span class="week-day-txt">Saturday</span></td>
								<td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td>
								<td class="week-day-val"><span class="plus">+</span>16&deg;</td>
								<td class="week-day-val"><span class="plus">+</span>4&deg;</td>
							</tr>
							<tr>
								<td class="week-day"><span class="week-day-txt">Sunday</span></td>
								<td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td>
								<td class="week-day-val"><span class="plus">+</span>19&deg;</td>
								<td class="week-day-val"><span class="plus">+</span>6&deg;</td>
							</tr>
						</table>
						<div class="booked-wzs-center">
							<span class="booked-wzs-bottom-l">See 7-Day Forecast</span>
						</div>
					</a>
				</div>
			</div>
			<script type="text/javascript">
				var css_file = document.createElement("link");
				css_file.setAttribute("rel", "stylesheet");
				css_file.setAttribute("type", "text/css");
				css_file
						.setAttribute("href",
								'https://s.bookcdn.com/css/w/booked-wzs-widget-160x275.css?v=0.0.1');
				document.getElementsByTagName("head")[0].appendChild(css_file);
				function setWidgetData(data) {
					if (typeof (data) != 'undefined' && data.results.length > 0) {
						for (var i = 0; i < data.results.length; ++i) {
							var objMainBlock = document
									.getElementById('m-booked-bl-simple-week-vertical-54751');
							if (objMainBlock !== null) {
								var copyBlock = document
										.getElementById('m-bookew-weather-copy-'
												+ data.results[i].widget_type);
								objMainBlock.innerHTML = data.results[i].html_code;
								if (copyBlock !== null)
									objMainBlock.appendChild(copyBlock);
							}
						}
					} else {
						alert('data=undefined||data.results is empty');
					}
				}
			</script>
			<script type="text/javascript" charset="UTF-8"
				src="https://widgets.booked.net/weather/info?action=get_weather_info&ver=6&cityID=3148&type=4&scode=124&ltid=3457&domid=w209&anc_id=56019&cmetric=1&wlangID=1&color=137AE9&wwidth=160&header_color=ffffff&text_color=333333&link_color=08488D&border_form=1&footer_color=ffffff&footer_text_color=333333&transparent=0"></script>
			<!-- weather widget end -->
		</div>
</div>
 <!-- <div id=footer>

<footer class="panel-footer">
  <div class="container">
    <div class="row">
      <section id="members" class="col-sm-4">
        
        Xianfeng Cai: samuel.caiceo@gmail.com<br>
        Diana  Yamaletdinova:  d.yamaletdinova@gmail.com<br>
        Bizen Okbaldet: Bizentse@gmail.com
        <hr class="visible-xs">
      </section>
      <section id="copyright" class="col-sm-4">
     
      </section>
      <section id="sayings" class="col-sm-4">
        <em>"It's a Mark of an educated mind, to entertain a thought without accepting it" --Aristotle </em>
      </section>
    </div>
    <div class="text-center">&copy; Copyright Amazing3 2017</div>
  </div>
</footer> 
</div>  -->

</body>
</html>

