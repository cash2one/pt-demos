// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.


/**
 * Returns a handler which will open a new window when activated.
 */
function getClickHandler() {
  return function(info, tab) {
    alert(info.menuItemId)
    // The srcUrl property is only available for image elements.
    var url = 'info.html#' + info.srcUrl;
    alert(info.linkUrl);
    // Create a new window to the info page.
    //chrome.windows.create({ url: url, width: 520, height: 660 });
  };
};

function clack_same()
{
    return function(info, tab)
    {
        if(info.linkUrl != 'undefine')
        {
            var patten=/www.meilishuo.com\/share\/item\/(\d{1,})/;
            var rs = patten.exec(info.linkUrl);
            if(rs.length == 2)
            {
              chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {  
                  chrome.tabs.sendMessage(tabs[0].id, {tid: rs[1]},function(response) {
                     console.log(response);  
                   }); 
              });
            }
            // window.open(info.linkUrl);
        }
    }
}

var contexts = ["page","selection","link","editable","image","video",
                "audio"];

/**
 * Create a context menu which will only show up for images.
 */
// var parent = chrome.contextMenus.create({"title" : "运营工具","type" : "normal","contexts" : contexts});
// "onclick" : getClickHandler()

var same = chrome.contextMenus.create({
    "title": " 标记同款", 
    // "parentId": parent,
    "contexts" : contexts,
    "type" : "normal",
    "onclick": clack_same()
});







