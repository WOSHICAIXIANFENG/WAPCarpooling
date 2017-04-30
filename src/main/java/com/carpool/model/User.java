package com.carpool.model;

import java.io.Serializable;

public class User implements Serializable {
	private static final long serialVersionUID = -2382582845887594309L;
	
	private int userid;
	private String fullname;
	private boolean gender;
	private String state;
	private String city;
	private String street;
	private int zipcode;
	private int birthyear;
	private String email;
	private String password;
	
	public User() {
		
	}
	
	public User(int userid, String fullname, boolean gender, String state, String city, String street, int zipcode,
			int birthyear, String email, String password) {
		super();
		this.userid = userid;
		this.fullname = fullname;
		this.gender = gender;
		this.state = state;
		this.city = city;
		this.street = street;
		this.zipcode = zipcode;
		this.birthyear = birthyear;
		this.email = email;
		this.password = password;
	}

	public int getUserid() {
		return userid;
	}

	public void setUserid(int userid) {
		this.userid = userid;
	}

	public String getFullname() {
		return fullname;
	}

	public void setFullname(String fullname) {
		this.fullname = fullname;
	}

	public boolean isGender() {
		return gender;
	}

	public void setGender(boolean gender) {
		this.gender = gender;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public int getZipcode() {
		return zipcode;
	}

	public void setZipcode(int zipcode) {
		this.zipcode = zipcode;
	}

	public int getBirthyear() {
		return birthyear;
	}

	public void setBirthyear(int birthyear) {
		this.birthyear = birthyear;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
	
	
}
