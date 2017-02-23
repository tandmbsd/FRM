function ExecuteAction(entityId, entityName, requestName, inputArg, callback) {
    // Creating the request XML for calling the Action
    function GetClientUrl() {
        if (typeof Xrm.Page.context == "object") {
            clientUrl = Xrm.Page.context.getClientUrl();
        }
        var ServicePath = "/XRMServices/2011/Organization.svc/web";
        return clientUrl + ServicePath;
    }
    var requestXML = [];
    requestXML.push("<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">");
    requestXML.push("<s:Body>");
    requestXML.push("<Execute xmlns=\"http://schemas.microsoft.com/xrm/2011/Contracts/Services\" xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">");
    requestXML.push("<request xmlns:a=\"http://schemas.microsoft.com/xrm/2011/Contracts\">");
    requestXML.push("<a:Parameters xmlns:b=\"http://schemas.datacontract.org/2004/07/System.Collections.Generic\">");
    if (inputArg != null && inputArg.length > 0) {
        for (var i = 0; i < inputArg.length; i++) {
            var tmp = inputArg[i];
            //var tmp = { name: '',type:'',value:'' };
            requestXML.push("<a:KeyValuePairOfstringanyType>");
            requestXML.push("<b:key>" + tmp.name + "</b:key>");
            requestXML.push("<b:value i:type=\"c:" + tmp.type + "\" xmlns:c=\"http://www.w3.org/2001/XMLSchema\">" + tmp.value + "</b:value>");
            requestXML.push("</a:KeyValuePairOfstringanyType>");
        }
    }
    requestXML.push("<a:KeyValuePairOfstringanyType>");
    requestXML.push("<b:key>Target</b:key>");
    requestXML.push("<b:value i:type=\"a:EntityReference\">");
    requestXML.push("<a:Id>" + entityId + "</a:Id>");
    requestXML.push("<a:LogicalName>" + entityName + "</a:LogicalName>");
    requestXML.push("<a:Name i:nil=\"true\" />");
    requestXML.push("</b:value>");
    requestXML.push("</a:KeyValuePairOfstringanyType>");
    requestXML.push("</a:Parameters>");
    requestXML.push("<a:RequestId i:nil=\"true\" />");
    requestXML.push("<a:RequestName>" + requestName + "</a:RequestName>");
    requestXML.push("</request>");
    requestXML.push("</Execute>");
    requestXML.push("</s:Body>");
    requestXML.push("</s:Envelope>");
    var req = new XMLHttpRequest();
    req.open('POST', GetClientUrl(), true);
    req.setRequestHeader('Accept', 'application/xml, text/xml, */*');
    req.setRequestHeader('Content-Type', 'text/xml; charset=utf-8');
    req.setRequestHeader('SOAPAction', 'http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute');
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            if (req.status == 200) {
                debugger;
                var result = {
                    status: 'success',
                    data: null
                }
                if (req.response != null && req.response.length > 0) {
                    try {
                        var rs = $.parseXML(req.response).getElementsByTagName("KeyValuePairOfstringanyType");
                        if (rs.length == 0)
                            rs = $.parseXML(req.response).getElementsByTagName("a:KeyValuePairOfstringanyType");
                        var len = rs.length;
                        if (len > 0) {
                            result.data = {};
                            for (var i = 0; i < len; i++) {
                                var key = rs[i].firstElementChild;
                                var sib = key.nextElementSibling;
                                result.data[key.textContent] = {
                                    type: sib.hasAttribute("i:type") ? sib.attributes["i:type"].value : '',
                                    value: key.nextElementSibling.textContent
                                };
                            }
                        }
                    }
                    catch (ex) {
                        result.status = "error";
                        result.data = ex.message;
                        console.log(ex.message)
                    }
                }
                if (callback != null)
                    callback(result);
            }
            else if (req.status == 500) {
                if (req.responseXML != "") {
                    var mss = req.responseXML.getElementsByTagName("Message");
                    if (mss.length > 0) {
                        if (callback != null)
                            callback({ status: "error", data: mss[0].firstChild.nodeValue });
                        console.log(mss[0].firstChild.nodeValue);
                    }
                }
                else if (callback != null)
                    callback(null);
            }
        }
    };
    req.send(requestXML.join(''));
}