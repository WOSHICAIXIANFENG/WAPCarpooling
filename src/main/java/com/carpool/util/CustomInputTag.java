package com.carpool.util;

import java.io.IOException;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;

/**
 * Make a custom tag for all input box.
 * <div class="form-group ">
    <label for="fullname">Full Name: </label>
    <input class="form-control" id="fullname" type="text" name="fullname" value="${LoginUser.fullname }" pattern="^[a-zA-Z]+\s[a-zA-Z]+$" title="use firstname lastname" required>
  </div>
  
  to:
   <tl:input name="fullname" label="Full Name: " id="fullname" type="text" class="form-control" value="${LoginUser.fullname }" pattern="^[a-zA-Z]+\s[a-zA-Z]+$"> </tl:input>
 * 
 * @author Xianfeng Cai
 */
public class CustomInputTag extends SimpleTagSupport {
	private boolean required;
	private String name; // ---- <label for="xx"> <input name="name">
	private String label;
	private String id;
	private String type;
	private String classStr;
	private String pattern;
	private String value;
	private int min = Integer.MIN_VALUE;// for number input ---- min set --- h5
	private String title; // for h5 error tips

	public void doTag() throws JspException, IOException {
		JspWriter out = getJspContext().getOut();
		
		String html = "<div class='form-group'><label for='%s'>%s</label> "
				+ "<input class='form-control %s' id='%s' type='%s' name='%s' value='%s' pattern='%s' "
				+ (min != Integer.MIN_VALUE ? "min='" + min + "' max='2999' " : " ") 
				+ ((title != null && !title.isEmpty())? "title='" + title + "'" : " ") 
				+ (required ? "required >" : " >")
				+ "</div>";
		
		out.write(String.format(html, name, label, classStr != null ? classStr : "" , 
				id, type, name, value, pattern != null ? pattern : "", required));
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getClassStr() {
		return classStr;
	}

	public void setClassStr(String classStr) {
		this.classStr = classStr;
	}

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	
}
