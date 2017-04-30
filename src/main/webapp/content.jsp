<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
  
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<meta charset="utf-8">
	<title>CarPooling</title>
	<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
  <link rel="stylesheet" type="text/css" href="style.css">
  <script type="text/javascript">
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
        tablinks[i].className = tablinks[i].className.replace(" active", "");
    }

    // Show the current tab, and add an "active" class to the button that opened the tab
    document.getElementById(postName).style.display = "block";
    evt.currentTarget.className += " active";
}

    $(function(){
    	let url = "http://www.wunderground.com/cgi-bin/findweather/getForecast?query=";
        $("#check_weather").click(function(){
        	//window.location=url + $("#destination").val();
        	$(location).attr("href", url + $("#destination").val());
        });
    });
</script>
  

</head>
<body>
<header>
  <nav id="header-nav" class="navbar navbar-default">
    <div class="container"></div>
    <div class="navbar-header">
      <a href="index.html" class="pull-left visible-md visible-lg">
        <div id="logo-img" alt="Logo image"></div>
      </a>
      <div class="navbar-brand">
        <a href="index.html"><h1>MUM CarPooling</h1></a>
      </div>
    </div>
  </nav>
</header>
<div id="navdiv">
  <ul class="nav"> 
  <li>
    <a href="#">Destination</a> 
    <ul> 
      <input type="text" id="destination" placeholder="city, State/Country" required />
      <li><a id='check_weather' title='Location Weather Forecast'>Click for weather forecast</a></li>
     </ul>
  </li>
 <li>
    <a href="#">Membership</a> 
    <ul> 
      <li><a href="#">Benefits & Membership Levels</a></li>
      <li><a href="#">Gift Memberships</a></li>
      <li><a href="#">Join or Renew</a></li>
      <li><a href="#">FAQ</a></li>
    </ul>
  </li>
</ul>
<div id="profile">
<ul class="nav">
  <li>
  <a>${"username"}</a>
  <ul>
  <li><a href="#">Edit</a></li>
  <li><a href="LoginServlet">Logout</a></li>
  </ul>
</ul>
</div>
</div>

<div class="tab">
 <button class="tablinks" onclick="openPost(event,'offeride')">Offer Ride <span style="color: red">${counterO}</span></button>
  <button class="tablinks" onclick="openPost(event,'getride')">Ask For A Ride <span style="color: red">${counterA}</button>
</div>
<div id="offeride" class="tabcontent"> your post for offer <br/>
<div id="monster">Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras vitae suscipit arcu. Praesent pretium orci neque, non egestas massa suscipit non. In urna ligula, pretium ac magna in, consectetur venenatis dui. Etiam id commodo neque, vel semper nunc. Vivamus porttitor condimentum pulvinar. Quisque et consequat mi. Suspendisse luctus, quam in dapibus venenatis, velit erat malesuada lacus, dapibus tincidunt neque ex vitae leo. Suspendisse fermentum sit amet urna eu dignissim. Curabitur vel nibh quis justo volutpat porttitor et tempus sem.
</div>
<div class="hideme">
In a luctus purus, in tempus mi. Integer vulputate tincidunt arcu quis aliquet. Maecenas sollicitudin nec nisi sit amet dictum. Curabitur sagittis nulla id sem vulputate, eget blandit nibh ullamcorper. Nam feugiat elementum pharetra. Vestibulum a purus eget mi mattis tincidunt a sed felis. Sed pretium dignissim elementum. Cras est arcu, posuere et justo in, vehicula rutrum elit. Phasellus dictum risus libero, non cursus neque faucibus a. Nunc dignissim at purus vitae condimentum. Curabitur in libero mi.
</div>
<div class="hideme">
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed at tristique nibh, sed congue ligula. Curabitur eu porttitor tellus. Aliquam eu mollis tortor. Donec tortor sapien, molestie eu turpis vel, ultrices pulvinar orci. Aenean sagittis sem sit amet viverra maximus. Morbi id enim ipsum. Curabitur luctus placerat erat ut volutpat. In quis eros mattis, rutrum neque ut, malesuada neque.
</div>
<div class="hideme">
Integer erat eros, vestibulum at tortor vitae, sollicitudin finibus est. Aliquam ornare, elit nec gravida sagittis, sapien nibh elementum felis, eu eleifend eros lectus non mi. Nulla vel nisl scelerisque, consectetur nibh vel, malesuada lacus. Nam lobortis accumsan nisl consequat dictum. Praesent eget lobortis lorem. Ut sed ultrices enim. Nam nec ultricies felis.
</div>
Donec hendrerit dolor id auctor ullamcorper. Curabitur ut mauris dolor. Quisque vitae cursus eros, ac rutrum sem. Aenean in turpis turpis. Fusce sit amet libero id massa dictum fermentum at eget arcu. Vestibulum eget blandit urna. In eu tristique augue. Phasellus augue risus, porttitor vel arcu nec, tincidunt laoreet tellus. Nam ornare leo dapibus ipsum dictum interdum.

Nulla molestie porttitor justo vitae pharetra. Proin non convallis lacus, eget malesuada metus. Duis aliquam eu massa molestie rhoncus. Vestibulum a malesuada nulla. Morbi at libero tempus, hendrerit quam vitae, auctor eros. Vivamus tincidunt enim a est tincidunt, sed fringilla erat placerat. Nulla cursus, eros sed posuere sagittis, dui est lobortis tellus, id dapibus dui sem eget enim. Vestibulum eleifend lacus velit, ut suscipit nisi bibendum at. Nulla facilisi. Aenean luctus tellus eget nisi vestibulum, eget interdum lectus efficitur.

Quisque facilisis aliquet dui, ut blandit odio vulputate et. Ut ac nisl turpis. Pellentesque scelerisque massa sit amet ipsum commodo cursus. Aenean eget ante et neque gravida tempor. Phasellus aliquam, purus quis malesuada vestibulum, sem mi cursus justo, a convallis purus dolor non lorem. Nunc dapibus vehicula nisi, eget egestas tellus lacinia vel. Nullam nisl ipsum, vehicula dignissim feugiat eu, semper nec arcu. Duis porttitor ut ex eget commodo. Curabitur accumsan diam ac euismod tincidunt. Cras dui urna, volutpat quis vehicula vitae, rhoncus a lacus. Curabitur ut purus aliquet, venenatis felis in, laoreet massa. Nullam lobortis sollicitudin aliquam. Quisque nec nisl eu sem vulputate venenatis. Proin sagittis erat sit amet sem vestibulum vehicula. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas.</div>
<div id="getride" class="tabcontent"> Ask for ride post</div>

<div id="weatherform">
    <!-- weather widget start --><div id="m-booked-bl-simple-week-vertical-54751"> <div class="booked-wzs-160-275 weather-customize" style="background-color:#137AE9; width:160px;" id="width1 " > <a target="_blank" class="booked-wzs-top-160-275" href="http://www.booked.net/"><img src="//s.bookcdn.com/images/letter/s5.gif" alt="http://www.booked.net/" /></a> <div class="booked-wzs-160-275_in"> <div class="booked-wzs-160-275-data"> <div class="booked-wzs-160-275-left-img wrz-01"></div> <div class="booked-wzs-160-275-right"> <div class="booked-wzs-day-deck"> <div class="booked-wzs-day-val"> <div class="booked-wzs-day-number"><span class="plus">+</span>24</div> <div class="booked-wzs-day-dergee"> <div class="booked-wzs-day-dergee-val">&deg;</div> <div class="booked-wzs-day-dergee-name">C</div> </div> </div> <div class="booked-wzs-day"> <div class="booked-wzs-day-d"><span class="plus">+</span>25&deg;</div> <div class="booked-wzs-day-n"><span class="plus">+</span>16&deg;</div> </div> </div> <div class="booked-wzs-160-275-info"> <div class="booked-wzs-160-275-city">Fairfield</div> <div class="booked-wzs-160-275-date">Monday, 24</div> </div> </div> </div> <a target="_blank" href="http://www.booked.net/weather/fairfield-3148" class="booked-wzs-bottom-160-275" > <table cellpadding="0" cellspacing="0" class="booked-wzs-table-160"> <tr> <td class="week-day"> <span class="week-day-txt">Tuesday</span></td> <td class="week-day-ico"><div class="wrz-sml wrzs-06"></div></td> <td class="week-day-val"><span class="plus">+</span>25&deg;</td> <td class="week-day-val"><span class="plus">+</span>13&deg;</td> </tr> <tr> <td class="week-day"> <span class="week-day-txt">Wednesday</span></td> <td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td> <td class="week-day-val"><span class="plus">+</span>27&deg;</td> <td class="week-day-val"><span class="plus">+</span>11&deg;</td> </tr> <tr> <td class="week-day"> <span class="week-day-txt">Thursday</span></td> <td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td> <td class="week-day-val"><span class="plus">+</span>13&deg;</td> <td class="week-day-val"><span class="plus">+</span>5&deg;</td> </tr> <tr> <td class="week-day"> <span class="week-day-txt">Friday</span></td> <td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td> <td class="week-day-val"><span class="plus">+</span>14&deg;</td> <td class="week-day-val"><span class="plus">+</span>7&deg;</td> </tr> <tr> <td class="week-day"> <span class="week-day-txt">Saturday</span></td> <td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td> <td class="week-day-val"><span class="plus">+</span>16&deg;</td> <td class="week-day-val"><span class="plus">+</span>4&deg;</td> </tr> <tr> <td class="week-day"> <span class="week-day-txt">Sunday</span></td> <td class="week-day-ico"><div class="wrz-sml wrzs-18"></div></td> <td class="week-day-val"><span class="plus">+</span>19&deg;</td> <td class="week-day-val"><span class="plus">+</span>6&deg;</td> </tr> </table> <div class="booked-wzs-center"> <span class="booked-wzs-bottom-l">See 7-Day Forecast</span> </div> </a> </div> </div><script type="text/javascript"> var css_file=document.createElement("link"); css_file.setAttribute("rel","stylesheet"); css_file.setAttribute("type","text/css"); css_file.setAttribute("href",'https://s.bookcdn.com/css/w/booked-wzs-widget-160x275.css?v=0.0.1'); document.getElementsByTagName("head")[0].appendChild(css_file); function setWidgetData(data) { if(typeof(data) != 'undefined' && data.results.length > 0) { for(var i = 0; i < data.results.length; ++i) { var objMainBlock = document.getElementById('m-booked-bl-simple-week-vertical-54751'); if(objMainBlock !== null) { var copyBlock = document.getElementById('m-bookew-weather-copy-'+data.results[i].widget_type); objMainBlock.innerHTML = data.results[i].html_code; if(copyBlock !== null) objMainBlock.appendChild(copyBlock); } } } else { alert('data=undefined||data.results is empty'); } } </script> <script type="text/javascript" charset="UTF-8" src="https://widgets.booked.net/weather/info?action=get_weather_info&ver=6&cityID=3148&type=4&scode=124&ltid=3457&domid=w209&anc_id=56019&cmetric=1&wlangID=1&color=137AE9&wwidth=160&header_color=ffffff&text_color=333333&link_color=08488D&border_form=1&footer_color=ffffff&footer_text_color=333333&transparent=0"></script><!-- weather widget end -->
  </div>  

</body>
</html>
