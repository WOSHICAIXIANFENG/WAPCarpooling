<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
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
   
  </script>
<body>
<header>
  <nav id="header-nav" class="navbar navbar-default">
    <div class="container"></div>
    <div class="navbar-header">
      <a href="index.jsp" class="pull-left visible-md visible-lg">
        <div id="logo-img" alt="Logo image"></div>
      </a>
      <div class="navbar-brand">
        <a href="index.jsp"><h1>MUM CarPooling</h1></a>
      </div>
    </div>
  </nav>
</header>


<div class="container" id="logindiv">
<div class="row">
   <div class="col-lg-4">
  <form action="LoginServlet" method="post">
  <div class="form-group ">
    
   <div class="form-group ">
   <label for="email">Email: </label>
   <input class="form-control" id="email" type="text" name="email" value="${cookie.user.value}" pattern="^[a-zA-Z_\-][a-zA-Z_\-0-9]+@(([a-zA-Z_\-])+\.)+[a-zA-Z]{2,4}$" value="${useremail}" required>
   </div>
   <div class="form-group ">
   <label for="password">Password: </label>
   <input class="form-control" id="password" type="password" name="password" value=""  pattern="^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?!.*\s).*$" required>
   </div>
   
   <div class="form-group ">
   <label >
		Remember Me: <input name="rememberMe" type="checkbox" <c:if test="${cookie.containsKey('user')}">checked</c:if> />
   </label>
   </div>
		
   <input type="hidden" name="hidden" value="index"/>
   <div class="form-group" class="btn btn-default">
   </div>
    <input type="hidden" name="hidden" value="index"/>
    <input type="submit"class="btn btn-info"  name="submit" value="Sign-in">
    <input type="button" class="btn btn-info"id="signup" onclick="JavaScript:window.location='signupjsp.jsp';" value="Sign-up">
    </div>
</form>
<span style="color: red">${error_msg}</span>
</div>
</div>
</div>
 <div id=footer>
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
</div>  
</body>
</html>
