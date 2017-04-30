package com.carpool.util;

/**
 * One util class used to encapsulate the result from XxxService
 * @author Samuel
 *
 */
public class ResultBean {
	private boolean success;
	private String msg; // error msg --- use by view
	private int code; // result code --- api level
	private Object obj; // optional obj --- save model
	
	public ResultBean(boolean success, String msg, int code) {
		super();
		
		this.code = code;
	}
	
	public ResultBean(boolean success, String msg) {
		this.success = success;
		this.msg = msg;
	}
	
	public ResultBean(boolean success) {
		this.success = success;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}
	
	public Object getObj() {
		return obj;
	}

	public void setObj(Object obj) {
		this.obj = obj;
	}

	@Override
	public String toString() {
		return "ResultBean [success=" + success + ", msg=" + msg + ", code=" + code + ", obj=" + obj + "]";
	}
}
