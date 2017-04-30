
<jsp:include page="header.jsp"></jsp:include>
<jsp:include page="navbar.jsp"></jsp:include>
</div>

<div id="body">

<div id="fb-root"></div>

<script>
(function(d, s, id) {
  var js, fjs = d.getElementsByTagName(s)[0];
  if (d.getElementById(id)) return;
  js = d.createElement(s); js.id = id;
  js.src = "//connect.facebook.net/en_US/sdk.js#xfbml=1&version=v2.9";
  fjs.parentNode.insertBefore(js, fjs);
}(document, 'script', 'facebook-jssdk'));
</script>
	
<!-- start of tabs -->

	<div class="wrapper_tab">
		<div class="container_tab">
			<div id="tabs">
				<ul style="height:30px;">
					<li style="height:30px;"><a href="#register">Offered Rides</a></li>
					<li style="height:30px;"><a href="#login">Ride Requests</a></li>	
					<li style="height:30px;"><a href="#newRequest">Add Requests</a></li>
				</ul>
				<div id="register">				
					<span id="request"> </span>
				</div>
				<div id="login">				
					<span id="offer"> </span>
				</div>
				<div id="newpost">				
					<span id="add request"> </span>
				</div>
			</div>
		</div>
	</div><!-- End of tabs -->
	
	<div id="weatherform"></div>
	<!-- weather widget start -->
	<div id="m-booked-bl-simple-week-vertical-54751">
		<div class="booked-wzs-160-275 weather-customize"
			style="background-color: #137AE9; width: 160px;" id="width1 ">
			<a target="_blank" class="booked-wzs-top-160-275"
				href="http://www.booked.net/"><img
				src="//s.bookcdn.com/images/letter/s5.gif"
				alt="http://www.booked.net/" /></a>
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
									<div class="booked-wzs-day-dergee-name"></div>
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
	
	<div>
		
<jsp:include page="footer.jsp"></jsp:include>
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