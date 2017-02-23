function btnProcApproveClick() {

}

function btnProcRejectClick() {

}

function btnProcCancelClick() {

}

function btnProcApproveEnable() {
    return false;
}

function btnProcCancelEnable() {
    return false;
}

function btnRejectEnable() {
    return false;
}

function addjs(path, id) {
    var script = document.getElementById(id);
    if (script == null) {
        script = document.createElement("script");
        script.id = id;
        script.src = Xrm.Page.context.getClientUrl() + "/" + path;
        document.head.appendChild(script);
    }
}


function SetButton(button, name) {
    var ent = Xrm.Page.data.entity.getEntityName();
    var btnId = "";
    var btn = null;
    var counter = 0;
    var timer = null;
    switch (button) {
        case 'approve':
            btnId = ent + '|NoRelationship|Form|new.ApplicationRibbon.' + ent + '.btnProcApprove.Button';
            break;
        case 'reject':
            btnId = ent + '|NoRelationship|Form|new.ApplicationRibbon.' + ent + '.btnProcReject.Button';
            break;
        case 'cancel':
            btnId = ent + '|NoRelationship|Form|new.ApplicationRibbon.' + ent + '.btnProcCancel.Button';
            break;
    }
    function setText() {
        btn = window.parent.document.getElementById(btnId);
        if (counter > 30) {
            if (timer != null) {
                clearTimeout(timer);
                timer = null;
            }
        }
        else {
            counter++;
            if (btn != null) {
                if (timer != null) {
                    clearTimeout(timer);
                    timer = null;
                }
                btn.firstElementChild.firstElementChild.lastElementChild.innerHTML = name;
            }
            else
                timer = setTimeout(setText, 1000);
        }
    };
    setText();
}

function GetRoleName(roleIds) {
    debugger;
    var serverUrl = location.protocol + "//" + location.host + "/" + Xrm.Page.context.getOrgUniqueName();
    var odataSelect = serverUrl + "/XRMServices/2011/OrganizationData.svc" + "/" + "RoleSet?$select=Name";
    var cdn = "";
    if (roleIds != null && roleIds.length > 0) {
        for (var i = 0; i < roleIds.length; i++) {
            if (i == roleIds.length - 1)
                cdn += "RoleId eq guid'" + roleIds[i] + "'";
            else
                cdn += "RoleId eq guid'" + roleIds[i] + "' or ";
        }
        if (cdn.length > 0)
            cdn = "&$filter=" + cdn;
        odataSelect += cdn;
        var roleName = [];
        $.ajax(
            {
                type: "GET",
                async: false,
                contentType: "application/json; charset=utf-8",
                datatype: "json",
                url: odataSelect,
                beforeSend: function (XMLHttpRequest) { XMLHttpRequest.setRequestHeader("Accept", "application/json"); },
                success: function (data, textStatus, XmlHttpRequest) {
                    if (data.d != null && data.d.results != null) {
                        var len = data.d.results.length;
                        for (var k = 0; k < len; k++)
                            roleName.push(data.d.results[k].Name);
                    }
                },
                error: function (XmlHttpRequest, textStatus, errorThrown) { alert('OData Select Failed: ' + textStatus + errorThrown + odataSelect); }
            }
        );
    }

    return roleName;
}
function processHandle() {
    debugger;
    var curStage = Xrm.Page.data.process.getActiveStage();
    var fields = curStage.getSteps();
    var len = fields.getLength();
    for (var i = 0 ; i < len; i++) {
        var field = fields.get(i)
        var fieldName = field.getAttribute();
        if (fieldName != null) {
            var control = Xrm.Page.getControl("header_process_" + fieldName);
            if (control != null && control.getControlType() == "lookup") {
                control.setDisabled(true);
            }
        }
    }

    var etn = Xrm.Page.data != null ? Xrm.Page.data.entity.getEntityName() : null;
    var rnames = GetRoleName(Xrm.Page.context.getUserRoles());
    var procId = null;
    var stageId = null;
    var changeStage = function (id, callback) {
        Xrm.Page.getAttribute("stageid").setValue(id);
        Xrm.Page.data.save().then(function () {
            setTimeout(function () {
                window.location.reload(true);
            }, 500);
            //Xrm.Page.data.refresh();
        }, function (e) {
            console.log(e.message);
        });
    }

    if (Xrm.Page.data != null && Xrm.Page.data.process != null) {
        procId = Xrm.Page.data.process.getActiveProcess().getId();
        stageId = Xrm.Page.data.process.getActiveStage().getId();
    }

    if (etn != null && rnames != null && rnames.length > 0 && procId != null && stageId != null) {
        var fetchXml = [];
        fetchXml.push("<fetch mapping='logical' count='1' version='1.0'>");
        fetchXml.push("<entity name='new_processconfig'>");
        fetchXml.push("<attribute name='new_schemaname'/>")
        fetchXml.push("<attribute name='new_approvename' />");
        fetchXml.push("<attribute name='new_appprovestagename' />");
        fetchXml.push("<attribute name='new_approvestageid'/>");
        fetchXml.push("<attribute name='new_approvevaluename'/>");
        fetchXml.push("<attribute name='new_approvevalue'/>");
        fetchXml.push("<attribute name='new_approvepersonfield'/>");
        fetchXml.push("<attribute name='new_approvepersonlogical'/>");
        fetchXml.push("<attribute name='new_rejectname' />");
        fetchXml.push("<attribute name='new_rejectstageid' />");
        fetchXml.push("<attribute name='new_rejectstagename' />");
        fetchXml.push("<attribute name='new_rejectvaluename'/>");
        fetchXml.push("<attribute name='new_rejectvalue'/>");
        fetchXml.push("<attribute name='new_cancelname'/>");
        fetchXml.push("<attribute name='new_cancelvaluename'/>");
        fetchXml.push("<attribute name='new_cancelvalue'/>");
        fetchXml.push("<filter type='and'>");
        fetchXml.push("<condition attribute='new_entityname' operator='begins-with' value='" + etn + "'/>");
        fetchXml.push("<condition attribute='new_processid' operator='eq' value='" + procId + "'/>");
        fetchXml.push("<condition attribute='new_stageid' operator='eq' value='" + stageId + "'/>");
        fetchXml.push("<condition attribute='new_rolename' operator='in'>");
        rnames.forEach(function (obj) {
            fetchXml.push("<value>" + obj + "</value>");
        });
        fetchXml.push("</condition>");
        fetchXml.push("</filter>");
        fetchXml.push("</entity>");
        fetchXml.push("</fetch>");
        btnRejectEnable = function () {
            return false;
        }
        btnProcApproveEnable = function () {
            return false;
        }
        btnProcCancelEnable = function () {
            return false;
        }
        CrmFetchKit.Fetch(fetchXml.join(''), true).then(function (rs) {
            if (rs.length > 0) {
                rs.forEach(function (obj) {
                    var att = obj.attributes;
                    var etnSchema = att.new_schemaname != null ? att.new_schemaname.value : null;
                    var statusCode = Xrm.Page.getAttribute("statuscode") != null ? Xrm.Page.getAttribute("statuscode").getValue() : null;
                    var approve = {
                        label: att.new_approvename != null ? att.new_approvename.value : null,
                        stageId: att.new_approvestageid != null ? att.new_approvestageid.value : null,
                        person: att.new_approvepersonlogical != null ? att.new_approvepersonlogical.value : null,
                        value: att.new_approvevalue != null ? att.new_approvevalue.value : null

                    }

                    var reject = {
                        label: att.new_rejectname != null ? att.new_rejectname.value : null,
                        stageId: att.new_rejectstageid != null ? att.new_rejectstageid.value : null,
                        value: att.new_rejectvalue != null ? att.new_rejectvalue.value : null
                    }

                    var cancel = {
                        label: att.new_cancelname != null ? att.new_cancelname.value : null,
                        value: att.new_cancelvalue != null ? att.new_cancelvalue.value : null,//att.new_rejectstageid != null ? att.new_rejectstageid.value : null
                    }

                    btnProcApproveEnable = function () {
                        var valid = etnSchema != null && approve != null && approve.label != null && approve.label.length > 0 && approve.stageId != null && approve.stageId.length > 0 && cancel.value != statusCode;
                        if (valid === true) {
                            btnProcApproveClick = function () {

                                if (IsValidStage() === true) {
                                    window.top.$ui.Confirm("Xác nhận", "Bạn có chắc muốn " + approve.label.toLowerCase() + " không?", function () {
                                        var record = {};
                                        if (approve.value != null)
                                            Xrm.Page.getAttribute("new_tinhtrangduyet").setValue(parseInt(approve.value));
                                        if (approve.person != null) {
                                            Xrm.Page.getAttribute(approve.person).setSubmitMode("always");
                                            Xrm.Page.getAttribute(approve.person).setValue([{
                                                name: Xrm.Page.context.getUserName(),
                                                id: Xrm.Page.context.getUserId(),
                                                type: "8"
                                            }]);
                                        }
                                        Xrm.Page.data.process.moveNext(function (e) {
                                            if (e == "success") {
                                                Xrm.Page.ui.refreshRibbon();
                                                processHandle();
                                            }
                                            else if (e == "invalid")
                                                Xrm.Page.ui.setFormHtmlNotification("<strong>Duyệt không hợp lệ. Vui lòng thử lại!</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Tải lại trang' href='javascript:' onclick='window.location.reload(true)'>'Tải lại trang</a>", "INFO", "1");
                                            else if (e == "dirtyForm")
                                                Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lòng lưu lại!</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Ẩn thông báo' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\");'>[X]</a>", "INFO", "1");
                                        });
                                    }, null);
                                }

                                //Xrm.Page.data.save().then(function () {

                                //},
                                //function (e) {
                                //    console.log(e.message);
                                //});
                            };
                            SetButton("approve", approve.label);
                        }
                        else
                            btnProcApproveClick = function () { };
                        return valid;
                    }
                    btnRejectEnable = function () {
                        var valid = etnSchema != null && reject != null && reject.label != null && reject.label.length > 0 && reject.stageId != null && reject.stageId.length > 0 && cancel.value != statusCode;
                        if (valid === true) {
                            btnProcRejectClick = function () {
                                window.top.$ui.Confirm("Xác nhận", "Bạn có chắc muốn " + reject.label.toLowerCase() + " không?", function () {
                                    var params = new Array();
                                    params[0] = { name: 'Entity', type: 'string', value: Xrm.Page.data.entity.getEntityName() };
                                    params[1] = { name: 'ObjectId', type: 'string', value: Xrm.Page.data.entity.getId() };
                                    params[2] = { name: 'StageId', type: 'string', value: reject.stageId };
                                    params[3] = { name: 'Attribute', type: 'string', value: 'new_tinhtrangduyet' };
                                    params[4] = { name: 'Value', type: 'string', value: reject.value };

                                    ExecuteAction2(Xrm.Page.data.entity.getId(), Xrm.Page.data.entity.getEntityName(), "new_ChangeStageAction", params, function (err) {
                                        try {
                                            if (err.data.Return.value == "success") {
                                                window.location.reload(true);
                                            }
                                            else
                                                alert(err);
                                        }
                                        catch (ex) { }
                                    });
                                }, null);
                            }
                            SetButton("reject", reject.label);
                        }
                        else
                            btnProcRejectClick = function () { };
                        return valid;
                    }
                    btnProcCancelEnable = function () {
                        var valid = etnSchema != null && cancel != null && cancel.label != null && cancel.label.length > 0 && cancel.value != null && cancel.value != statusCode;
                        if (valid === true) {
                            btnProcCancelClick = function () {
                                window.top.$ui.Confirm("Xác nhận", "Bạn có chắc muốn " + cancel.label.toLowerCase() + " không?", function () {
                                    SetStateRecord(Xrm.Page.data.entity.getEntityName(), Xrm.Page.data.entity.getId(), cancel.value, function (err) {
                                        try {
                                            if (err.data == null || err.data.Return.value == "success") {
                                                window.location.reload(true);
                                            }
                                            else
                                                alert(err);
                                        }
                                        catch (ex) { }
                                    });
                                }, null);
                            }
                            SetButton("cancel", cancel.label);
                        }
                        else
                            btnProcCancelClick = function () { };
                        return valid;
                    }
                });
                setTimeout(function () {
                    Xrm.Page.ui.refreshRibbon();
                }, 1000);

            }
            else {
                btnRejectEnable = function () {
                    return false;
                }
                btnProcApproveEnable = function () {
                    return false;
                }
                btnProcCancelEnable = function () {
                    return false;
                }
                setTimeout(function () {
                    Xrm.Page.ui.refreshRibbon();
                }, 1000);
            }
        }, function (e) {
            console.log(e.message);
            btnRejectEnable = function () {
                return false;
            }
            btnProcApproveEnable = function () {
                return false;
            }
            btnProcCancelEnable = function () {
                return false;
            }
            setTimeout(function () {
                Xrm.Page.ui.refreshRibbon();
            }, 1000);
        });
    }
    else {
        btnRejectEnable = function () {
            return false;
        }
        btnProcApproveEnable = function () {
            return false;
        }
        btnProcCancelEnable = function () {
            return false;
        }
        setTimeout(function () {
            Xrm.Page.ui.refreshRibbon();
        }, 1000);
    }
}

function removeArrowButton() {
    var ac = $("#processActionsContainer");
    var cv = $("#collapsibleView");
    function fitSize() {
        var st = $("#processStagesContainer");
        st.width(st.parent().width());
    }
    if (ac != null && ac.length > 0) {
        ac.remove();
        if (cv != null && cv.length > 0)
            cv.remove();
        var proHeader = document.getElementById("processHeaderArea");
        if (proHeader != null) {
            var fakeProHeader = proHeader.cloneNode(true);
            proHeader.parentElement.replaceChild(fakeProHeader, proHeader);
        }
        fitSize();
        var timer_size = null;
        $(window).resize(function (w) {
            if (timer_size != null) {
                clearTimeout(timer_size);
                timer_size = null;
            }
            timer_size = setTimeout(function () { fitSize() }, 300);
        });
    }
}

function IsValidStage() {
    var flag = true;
    if (Xrm.Page.data.getIsValid() === true) {
        var curStage = Xrm.Page.data.process.getActiveStage();
        var fields = curStage.getSteps();
        var len = fields.getLength();
        for (var i = 0 ; i < len; i++) {
            var field = fields.get(i)
            var fieldName = field.getAttribute();
            if (field.isRequired() === true && fieldName != null) {
                var control = Xrm.Page.getControl("header_process_" + fieldName);
                if (control != null) {
                    var attr = control.getAttribute();
                    if (attr.getValue() == null) {
                        control.setFocus(true);
                        Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lòng nhập " + field.getName() + "!</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Ẩn thông báo' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\");'>[X]</a>", "INFO", "1");
                        setTimeout(function () {
                            Xrm.Page.ui.clearFormNotification("1");
                        }, 2000);
                        flag = false;
                        break;
                    }
                }
            }
        }
        return flag;
    }
    else
        return false;

}

function waitReady(callback) {
    var timer = null;
    var wait = function () {

        if (window["Xrm"] != null && Xrm.Page != null && Xrm.Page.context != null) {
            addjs("webresources/new_ribbon_process_crmfetchkit.js", "new_ribbon_process_crmfetchkit");
            addjs("webresources/new_process_SDK.REST.js", "new_process_SDK.REST");
            addjs("webresources/new_modal.utilities.js", "new_modal.utilities.js");
            if (typeof (CrmFetchKit) != "undefined") {
                if (timer != null) {
                    clearTimeout(timer);
                    timer = null;
                }
                if (callback != null)
                    callback();
            }
            else {
                timer = setTimeout(wait, 1000, callback);
                console.log(new Date());
            }
        }
        else {
            timer = setTimeout(wait, 1000, callback);
            console.log(new Date());
        }

    }
    wait();
}

$(document).ready(function (doc) {
    removeArrowButton();
    waitReady(function () {
        processHandle();
    });
});

function GetClientUrl() {
    if (typeof Xrm.Page.context == "object") {
        clientUrl = Xrm.Page.context.getClientUrl();
    }
    var ServicePath = "/XRMServices/2011/Organization.svc/web";
    return clientUrl + ServicePath;
}

function ExecuteAction2(entityId, entityName, requestName, inputArg, callback) {
    // Creating the request XML for calling the Action
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
    //requestXML.push("<a:KeyValuePairOfstringanyType>");
    //requestXML.push("<b:key>Target</b:key>");
    //requestXML.push("<b:value i:type=\"a:EntityReference\">");
    //requestXML.push("<a:Id>" + entityId + "</a:Id>");
    //requestXML.push("<a:LogicalName>" + entityName + "</a:LogicalName>");
    //requestXML.push("<a:Name i:nil=\"true\" />");
    //requestXML.push("</b:value>");
    //requestXML.push("</a:KeyValuePairOfstringanyType>");
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

function SetStateRecord(entityName, recordId, status, callback) {
    var requestMain = ""
    requestMain += "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">";
    requestMain += "  <s:Body>";
    requestMain += "    <Execute xmlns=\"http://schemas.microsoft.com/xrm/2011/Contracts/Services\" xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">";
    requestMain += "      <request i:type=\"b:SetStateRequest\" xmlns:a=\"http://schemas.microsoft.com/xrm/2011/Contracts\" xmlns:b=\"http://schemas.microsoft.com/crm/2011/Contracts\">";
    requestMain += "        <a:Parameters xmlns:c=\"http://schemas.datacontract.org/2004/07/System.Collections.Generic\">";
    requestMain += "          <a:KeyValuePairOfstringanyType>";
    requestMain += "            <c:key>EntityMoniker</c:key>";
    requestMain += "            <c:value i:type=\"a:EntityReference\">";
    requestMain += "              <a:Id>" + recordId + "</a:Id>";
    requestMain += "              <a:LogicalName>" + entityName + "</a:LogicalName>";
    requestMain += "              <a:Name i:nil=\"true\" />";
    requestMain += "            </c:value>";
    requestMain += "          </a:KeyValuePairOfstringanyType>";
    requestMain += "          <a:KeyValuePairOfstringanyType>";
    requestMain += "            <c:key>State</c:key>";
    requestMain += "            <c:value i:type=\"a:OptionSetValue\">";
    requestMain += "              <a:Value>0</a:Value>";
    requestMain += "            </c:value>";
    requestMain += "          </a:KeyValuePairOfstringanyType>";
    requestMain += "          <a:KeyValuePairOfstringanyType>";
    requestMain += "            <c:key>Status</c:key>";
    requestMain += "            <c:value i:type=\"a:OptionSetValue\">";
    requestMain += "              <a:Value>" + status + "</a:Value>";
    requestMain += "            </c:value>";
    requestMain += "          </a:KeyValuePairOfstringanyType>";
    requestMain += "        </a:Parameters>";
    requestMain += "        <a:RequestId i:nil=\"true\" />";
    requestMain += "        <a:RequestName>SetState</a:RequestName>";
    requestMain += "      </request>";
    requestMain += "    </Execute>";
    requestMain += "  </s:Body>";
    requestMain += "</s:Envelope>";
    var req = new XMLHttpRequest();
    req.open('POST', GetClientUrl(), true);
    req.setRequestHeader('Accept', 'application/xml, text/xml, */*');
    req.setRequestHeader('Content-Type', 'text/xml; charset=utf-8');
    req.setRequestHeader('SOAPAction', 'http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute');
    req.onreadystatechange = function () {
        if (req.readyState == 4) {
            if (req.status == 200) {
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
    req.send(requestMain);
}