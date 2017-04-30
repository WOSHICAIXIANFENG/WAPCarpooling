$(function() {
	$("div.form-group input").prop('readonly', true);
	$('div.form-group input[type="radio"]').attr('disabled',true);
	
	$("#edit_btn").css("margin-right", "100px");
	$("#save_btn").hide();
	$("#edit_btn").show();
	
	$("#edit_btn").click(function() {
		$("div.form-group input").prop('readonly', false);
		$('div.form-group input[type="radio"]').attr('disabled',false);
		$("#save_btn").show();
		$("#edit_btn").hide();
	});
	
	$("#cancel_btn").css("float", "right").click(function() {
		parent.history.back();
		return false;
	});
});