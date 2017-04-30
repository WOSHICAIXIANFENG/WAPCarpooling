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
  <h2>Edit Profile</h2>
  <div class="col-lg-3">
  <form action="ProfileController.do?action=edit&userId=<c:out value="${LoginUser.userid}" />" method="post">
  <div class="form-group ">
    <label for="fullname">Full Name: </label>
    <input class="form-control" id="fullname" type="text" name="fullname" pattern="^[a-zA-Z]+\s[a-zA-Z]$" title="use firstname lastname" value="${LoginUser.fullname }" required>
  </div>
  <div class="form-group ">
    <label>Female <input required type="radio" name="gender" value="F" <c:if test="${LoginUser.gender == false}">checked</c:if> ></label>
  <label>Male<input required type="radio" name="gender" value="M" <c:if test="${LoginUser.gender == true}">checked</c:if>></label>
  </div>
  <div class="form-group ">
  <label for="state">State: </label>
  <input class="form-control" id="state" type="text" name="state" pattern="^[a-zA-Z]*$" value="${LoginUser.state }" required>
  </div>
  <div class="form-group ">
  <label for="city">City: </label>
  <input class="form-control" id="city" type="text" name="city" pattern="^[a-zA-Z]+\s[a-zA-Z]$" value="${LoginUser.city }">
  </div>
  <div class="form-group ">
  <label for="street">Street: </label>
   <input class="form-control" id="street" type="text" name="street" pattern="[a-zA-Z]+\s+[a-zA-Z]+" value="${LoginUser.street }" required>
   </div>
   <div id="left">
   <div class="form-group">
  <label for="zipcode">Zip code: </label>
  <input class="form-control" id="zipcode" type="text" name="zipcode" pattern="^[0-9]{5}$" title="use 12345 formate" required></label>
   </div>
   <div class="form-group ">
   <label for="birthyear">Birth Year: </label>
   <input class="form-control" id="birthyear" type="text" name="birthyear" min="1999" value="${LoginUser.birthyear }" required>
   </div>
   <div class="form-group ">
   <label for="email">Email: </label>
   <input class="form-control" id="email" type="text" name="email" pattern="^[a-zA-Z_\-]+[a-zA-Z_\-0-9]+@(([a-zA-Z_\-])+\.)+[a-zA-Z]{2,4}$" required>
   </div>
   <div class="form-group ">
   <label for="password">Password: </label>
   <input class="form-control" value="${LoginUser.email }" id="password" type="password" name="password" pattern=(?=\d+)(?=[a-z]*)(?=[A-Z]*).{8,}" title="Must contain at east one number and one uppercase and lowercase letter, and at least 8 or more characters" required>
   </div>
     <div class="form-group" class="btn btn-default">
   </div>
   <input type="submit" name="submit" value="Save">
   </div>
</form>
</div>
</div>
</div>
     
</body>
</html>
