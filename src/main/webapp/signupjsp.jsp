<%@ taglib prefix="ct" uri="/WEB-INF/custom.tld" %>
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
</head>
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

<div class="container">
<div class="row">
  <h2>Creat Account</h2>
  <div class="col-lg-3">
  <form class="form-horizontal"action="ProfileController.do?action=create" method="post">
  <!-- <div class="form-group ">
    <label for="fullname">Full Name: </label>
    <input class="form-control" id="fullname" type="text" name="fullname" pattern="^[a-zA-Z]+\s[a-zA-Z]+$" title="use firstname lastname" required>
  </div> -->
  
  <ct:input label="Full Name: " value="" required='true' id="fullname" type="text" name="fullname" pattern="^[a-zA-Z]+\s[a-zA-Z]+$" />
  
  <div class="form-group ">
    <label>Female <input required type="radio" name="gender" value="F" <c:if test="${LoginUser.gender == false}">checked</c:if> ></label>
  <label>Male<input required type="radio" name="gender" value="M" <c:if test="${LoginUser.gender == true}">checked</c:if>></label>
  </div>
  
  <!-- <div class="form-group ">
  <label for="state">State: </label>
  <input class="form-control" id="state" type="text" name="state" pattern="^[a-zA-Z]+\s*[a-zA-Z]*$" required>
  </div> -->
  
  <ct:input required='true' value="" label="State: " id="state" type="text" name="state"  pattern="^[a-zA-Z]+\s*[a-zA-Z]*$" />
  
  
  <!-- <div class="form-group ">
  <label for="city">City: </label>
  <input class="form-control" id="city" type="text" name="city" pattern="^[a-zA-Z]+\s*[a-zA-Z]*$">
  </div> -->
  
  <ct:input required='true' value="" label="City: " id="city" type="text" name="city" pattern="^[a-zA-Z]+\s*[a-zA-Z]*$" />
  
<!--   <div class="form-group ">
  <label for="street">Street: </label>
   <input class="form-control" id="street" type="text" name="street" pattern="^[a-zA-Z0-9]+\s+[a-zA-Z0-9]+\s++[a-zA-Z0-9]+\s+[a-zA-Z]+$" required>
   </div> -->
   
   <ct:input required='true' label="Street: " id="street" type="text" name="street" value="" pattern="^[a-zA-Z0-9]+\s+[a-zA-Z0-9]+\s++[a-zA-Z0-9]+\s+[a-zA-Z]+$" />
   
   <div id="left">
<!--    <div class="form-group">
  <label for="zipcode">Zip code: </label>
  <input class="form-control" id="zipcode" type="text" name="zipcode" pattern="^[0-9]{5}$" title="use 5 digits formate" required></label>
   </div> -->
   
   <ct:input required='true' label="Zip code: " id="zipcode" type="number" name="zipcode" value="" pattern="^[0-9]{5}$" />
   
   <!-- <div class="form-group ">
   <label for="birthyear">Birth Year: </label>
   <input class="form-control" id="birthyear" type="text" name="birthyear" min="1999" required>
   </div> -->
   
   <ct:input required='true' label="Birth Year: " id="birthyear" type="number" name="birthyear" value="" min="1999" />
   
   <!-- <div class="form-group ">
   <label for="email">Email: </label>
   <input class="form-control" id="email" type="email" name="email" pattern="^[a-zA-Z_\-][a-zA-Z_\-0-9]+@(([a-zA-Z_\-])+\.)+[a-zA-Z]{2,4}$" required>
   </div> -->
   
   <ct:input required='true' label="Email: " id="email" type="email" name="email" value="" pattern="^[a-zA-Z_\-][a-zA-Z_\-0-9]+@(([a-zA-Z_\-])+\.)+[a-zA-Z]{2,4}$" />
    
   <div class="form-group ">
   <label for="password">Password: </label>
   <input class="form-control" id="password" type="password" name="password" pattern="^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?!.*\s).*$" title="Must contain at east one number and one uppercase and lowercase letter, and at least 8 or more characters" required>
   </div>
   <input type="hidden" name="hidden" value="signupjsp"/>
   <div class="form-group" class="btn btn-default">
   </div>
   <input class="btn btn-info" type="submit" name="submit" value="Sign-up">
   </div>
</form>
</div>
</div>
</div>

<!-- <footer class="panel-footer">
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
</footer>   -->
</body>
</html>
