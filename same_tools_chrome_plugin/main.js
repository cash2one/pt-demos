// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

/**
 * Returns a handler which will open a new window when activated.
 */
$(function(){
    // $.getScript('http://localhost:8084/assets/js/background.js');
    $.getScript('http://works.meiliworks.com/app/sameVerify/same_tools.js');
    // $.get("http://baidu.com",function(result){
	uid=4699;
	chrome.runtime.onMessage.addListener(  function(request, sender, sendResponse){
	if(uid>0)
	{
		var len = $("#same_console_div").find(":checkbox").length;
		if(len>=10)
		{
			alert("一次最多添加10个同款");
			sendResponse( "ok"); 
			return false;
		}
		var exists = false;
		$("#same_console_div").find(":checkbox").each(function(index, obj){
			if(obj.value == request.tid)
			{
				exists = true;
				return false;
			}
		});
		if(exists)
		{
			alert(request.tid+"已经存在");
			sendResponse( "ok"); 
			return false;
		}
		var checkbox_div="<label style='display: block;'><input name='t_id' type='checkbox' value='"+request.tid+"' checked='checked'/><span>"+request.tid+"</span></lavel>";
		$("#same_console_div").append(checkbox_div);
		$("#same_console").show();
	}
	else
	{
		window.open("http://works.meiliworks.com/");
	}
	sendResponse( "ok"); 
	});
    // });
 });