// JavaScript source code

function btn_getPhieuTTmia() {
    debugger;
    $ui.Confirm("Question", "Bạn có muốn lấy phiếu ?", function () {
        processingDlg.show();
        Xrm.Page.data.save().then(function () {
            RunWorkflow("0B813F56-3C98-48D1-ADE1-8B8CFF934480", Xrm.Page.data.entity.getId(), function (req) {
                if (req.readyState == 4) {
                    processingDlg.hide();
                    if (req.status == 200) {
                        Xrm.Page.data.refresh();
                    }
                    else if (req.status == 500) {
                        if (req.responseXML != "") {
                            var mss = req.responseXML.getElementsByTagName("Message");
                            debugger;
                            if (mss.length > 0)
                                $ui.Dialog("Error", mss[0].firstChild.nodeValue, null);
                        }
                    }
                }
            });
        },
        function (error) {
            processingDlg.hide();
            $ui.Dialog("Error", error.message, null);
            console.log(error.message);
        });
    }, function () {

    });
}

function VisBtn_WonQuote() {
    var statuscode = Xrm.Page.getAttribute("statuscode").getValue() ? Xrm.Page.getAttribute("statuscode").getValue() : 0;
    if ((Xrm.Page.getAttribute("statecode").getValue() != 1) || (Xrm.Page.getAttribute("statuscode").getValue() == 4))
        return false;
    return true;
}

//-----------------------------------RUN WORKFLOW---------------------------------------------------
function RunWorkflow(workflowId, entityId, callback) {
    // var _return = window.confirm('Are you want to execute workflow.');
    //if (_return) {
    var url = 'http://' + window.location.host + '/' + Xrm.Page.context.getOrgUniqueName();//Xrm.Page.context.getServerUrl();
    // var entityId = Xrm.Page.data.entity.getId();
    //var workflowId = 'CFA66414-AA64-4831-B151-4357FB750F0B';
    var OrgServicePath = "/XRMServices/2011/Organization.svc/web";
    url = url + OrgServicePath;
    var request;
    request = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">" +
                  "<s:Body>" +
                    "<Execute xmlns=\"http://schemas.microsoft.com/xrm/2011/Contracts/Services\" xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">" +
                      "<request i:type=\"b:ExecuteWorkflowRequest\" xmlns:a=\"http://schemas.microsoft.com/xrm/2011/Contracts\" xmlns:b=\"http://schemas.microsoft.com/crm/2011/Contracts\">" +
                        "<a:Parameters xmlns:c=\"http://schemas.datacontract.org/2004/07/System.Collections.Generic\">" +
                          "<a:KeyValuePairOfstringanyType>" +
                            "<c:key>EntityId</c:key>" +
                            "<c:value i:type=\"d:guid\" xmlns:d=\"http://schemas.microsoft.com/2003/10/Serialization/\">" + entityId + "</c:value>" +
                          "</a:KeyValuePairOfstringanyType>" +
                          "<a:KeyValuePairOfstringanyType>" +
                            "<c:key>WorkflowId</c:key>" +
                            "<c:value i:type=\"d:guid\" xmlns:d=\"http://schemas.microsoft.com/2003/10/Serialization/\">" + workflowId + "</c:value>" +
                          "</a:KeyValuePairOfstringanyType>" +
                        "</a:Parameters>" +
                        "<a:RequestId i:nil=\"true\" />" +
                        "<a:RequestName>ExecuteWorkflow</a:RequestName>" +
                      "</request>" +
                    "</Execute>" +
                  "</s:Body>" +
                "</s:Envelope>";

    var req = new XMLHttpRequest();
    req.open("POST", url, true)
    // Responses will return XML. It isn't possible to return JSON.
    req.setRequestHeader("Accept", "application/xml, text/xml, */*");
    req.setRequestHeader("Content-Type", "text/xml; charset=utf-8");
    req.setRequestHeader("SOAPAction", "http://schemas.microsoft.com/xrm/2011/Contracts/Services/IOrganizationService/Execute");
    req.onreadystatechange = function () {
        debugger;
        if (callback != null)
            callback(req);
    };
    req.send(request);
    // }
}