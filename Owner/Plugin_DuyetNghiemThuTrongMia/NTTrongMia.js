function HopDongOnChange() {
    var hd = Xrm.Page.getAttribute("new_hopdongtrongmia").getValue();
    if (hd != null && hd.length > 0) {
        SDK.REST.retrieveRecord(
        hd[0].id,
        "new_hopdongdautumia",
        "new_tram,new_canbonongvu", null,
         function (hdd) {
             if (hdd.new_tram != null && hdd.new_tram.Id != null) {
                 Xrm.Page.getAttribute("new_tram").setValue([{
                     name: hdd.new_tram.Name,
                     id: hdd.new_tram.Id,
                     type: "10"
                 }]);
             }
             else
                 Xrm.Page.ui.setFormHtmlNotification("<strong>Hợp đồng đầu tư mía '" + hd[0].name + "' chưa chọn trạm nông vụ!</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Ẩn thông báo' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"4\");'>[X]</a>", "WARNING", "4");
             if (hdd.new_canbonongvu != null && hdd.new_canbonongvu.Id != null) {
                 Xrm.Page.getAttribute("new_canbonongvu").setValue([{
                     name: hdd.new_canbonongvu.Name,
                     id: hdd.new_canbonongvu.Id,
                     type: "10084"
                 }]);
             }
             else
                 Xrm.Page.ui.setFormHtmlNotification("<strong>Hợp đồng đầu tư mía '" + hd[0].name + "' chưa chọn cán bộ nông vụ!</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Ẩn thông báo' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"4\");'>[X]</a>", "WARNING", "4");
         },
         function (er) {
             console.log(er.message);
         });
    }
    else {
        Xrm.Page.getAttribute("new_canbonongvu").setValue(null);
        Xrm.Page.getAttribute("new_tram").setValue(null);
    }
}


function filter_hopdong_khachhang(isOnload) {
    var kh_c = Xrm.Page.getAttribute("new_khachhang").getValue();
    var kh_d = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
    var new_hopdong = Xrm.Page.getAttribute("new_hopdongtrongmia");
    if (kh_c != null && kh_c.length > 0) {
        if (isOnload === true) {
            var viewId = "{DBC69DF8-DE9E-E511-93F0-98BE942A7CB3}";
            var entityName = "new_hopdongdautumia";
            var viewDisplayName = "Hợp đồng đầu tư mía Lookup View";

            var fetchXml = "<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
                                "<entity name='new_hopdongdautumia'> " +
                                    "<attribute name='new_hopdongdautumiaid' /> " +
                                    "<attribute name='new_name' /> " +
                                    "<attribute name='createdon' /> " +
                                    "<filter type='and'> " +
                                        "<condition attribute='new_khachhang' operator='eq' value='" + kh_c[0].id + "' /> " +
                                    "</filter> " +
                                  "</entity> " +
                            "</fetch>   ";

            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_name'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                "<row name='result'  " + "id='new_hopdongdautumiaid'>  " +
                                "<cell name='new_name'   " + "width='200' />  " +
                                "<cell name='createdon'    " + "width='200' />  " +
                                "</row>   " +
                             "</grid>   ";

            Xrm.Page.getControl("new_hopdongtrongmia").addCustomView(viewId, entityName, viewDisplayName, fetchXml, layoutXml, true);
        }

    } else {
        if (isOnload === true) {
            if (kh_d == null || kh_d.length <= 0) {
                new_hopdong.setValue(null);
            }
        }
    }
}

function filter_hopdong_doanhnghiep(isOnload) {
    var kh_c = Xrm.Page.getAttribute("new_khachhang").getValue();
    var kh_d = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
    var new_hopdong = Xrm.Page.getAttribute("new_hopdongtrongmia");
    if (kh_d != null && kh_d.length > 0) {
        if (isOnload === true) {
            var viewId = "{DBC69DF8-DE9E-E511-93F0-98BE942A7CB3}";
            var entityName = "new_hopdongdautumia";
            var viewDisplayName = "Hợp đồng đầu tư mía Lookup View";

            var fetchXml = "<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> " +
                                "<entity name='new_hopdongdautumia'> " +
                                    "<attribute name='new_hopdongdautumiaid' /> " +
                                    "<attribute name='new_name' /> " +
                                    "<attribute name='createdon' /> " +
                                    "<filter type='and'> " +
                                        "<condition attribute='new_khachhangdoanhnghiep' operator='eq' value='" + kh_d[0].id + "' /> " +
                                    "</filter> " +
                                  "</entity> " +
                            "</fetch>   ";

            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_name'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                "<row name='result'  " + "id='new_hopdongdautumiaid'>  " +
                                "<cell name='new_name'   " + "width='200' />  " +
                                "<cell name='createdon'    " + "width='200' />  " +
                                "</row>   " +
                             "</grid>   ";

            Xrm.Page.getControl("new_hopdongtrongmia").addCustomView(viewId, entityName, viewDisplayName, fetchXml, layoutXml, true);
        }

    } else {
        if (isOnload === true) {
            if (kh_c == null || kh_c.length <= 0) {
                new_hopdong.setValue(null);
            }
        }
    }
}

function filter_thuacanhtac() {
    var hopdong = Xrm.Page.getControl("new_hopdongtrongmia").getAttribute().getValue();
    if (hopdong != null && hopdong.length > 0) {
        Xrm.Page.getControl("new_hopdongtrongmia").getAttribute().setValue(hopdong[0].id);
    } else {
        Xrm.Page.getControl("new_thuacanhtac").getAttribute().setValue(null);
    }
}
function filter_canbonongvu() {
    Xrm.Page.getControl("new_canbonongvu").getAttribute().setValue(null);
}

// load Lan nghiem thu 
function lannghiemthu() {
    var formType = Xrm.Page.ui.getFormType();
    if (formType == 1) {
        var hd = Xrm.Page.getControl("new_hopdongtrongmia").getAttribute().getValue();
        var kh = Xrm.Page.getAttribute("new_khachhang").getValue();
        var khdn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
        var lannghiemthu = Xrm.Page.getAttribute("new_lannghiemthu");
        var name = Xrm.Page.getAttribute("subject");
        if (hd != null && hd.length > 0) {
            /* Xrm.Page.getControl("subject").setDisabled(true);
             Xrm.Page.getControl("new_lannghiemthu").setDisabled(true);*/
            SDK.REST.retrieveMultipleRecords(
             "new_nghiemthutrongmia",
             "$select=ActivityId&$filter=new_hopdongtrongmia/Id eq guid'" + hd[0].id + "'",
                function (results) {
                    if (results.length > 0) {
                        if (kh != null) {
                            lannghiemthu.setValue(results.length + 1);
                            name.setValue("Nghiệm thu trồng mía " + kh[0].name + "- lần " + (results.length + 1));
                        }
                        if (khdn != null) {
                            lannghiemthu.setValue(results.length + 1);
                            name.setValue("Nghiệm thu trồng mía " + khdn[0].name + "- lần " + (results.length + 1));
                        }
                    }
                    else {
                        if (kh != null) {
                            lannghiemthu.setValue(1);
                            name.setValue("Nghiệm thu trồng mía " + kh[0].name + "- lần " + 1);
                        }
                        if (khdn != null) {
                            lannghiemthu.setValue(1);
                            name.setValue("Nghiệm thu trồng mía " + khdn[0].name + "- lần " + 1);
                        }
                    }
                },
                function (error) {
                    alert("Can't retrieve data !");
                },
                function () {

                }
           );
        } else if (hd == null) {
            lannghiemthu.setValue(null);
            name.setValue(null);
        }
    }
}

function count() {
    var hdtd = Xrm.Page.getAttribute("new_hopdongtrongmia").getValue();
    var xml = [];
    var soluong = 0;
    if (hdtd != null) {
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' aggregate='true' distinct='true' >");
        xml.push("<entity name='new_nghiemthutrongmia'> ");
        xml.push("<attribute name='activityid' alias='ntdv_count' aggregate='count' />");
        xml.push("<filter type='and'> ");
        xml.push("<condition attribute='new_hopdongtrongmia' operator='eq' value='" + hdtd[0].id + "' /> ");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            soluong = rs[0].attributes.ntdv_count.value;
        },
        function (er) {
            console.log(er.message)
        });
    }
    soluong = soluong + 1;
    return soluong;
}


function settieude() {
    if (Xrm.Page.ui.getFormType() == 1) {
        var tieude = "NT Trồng mía ";
        var kh = Xrm.Page.getAttribute("new_khachhang").getValue();
        var khdn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
        if (kh != null || khdn != null) {
            var soluong = count();
            if (kh != null && kh.length > 0) {
                tieude += " " + kh[0].name;
            }
            else if (kh == null || kh.length <= 0) {
                tieude += " " + khdn[0].name;
            }
            tieude += " " + " - lần " + soluong;
            Xrm.Page.getAttribute("subject").setValue(tieude);
        }
    }
}
function lock() {
    if (Xrm.Page.ui.getFormType() == 2) {
        Xrm.Page.getControl("new_multilookup").setDisabled(true);
        Xrm.Page.getControl("new_hopdongtrongmia").setDisabled(true);
    }
}

var timer = null;
function taophieudieuchinhcongno() {
    var trangthai = Xrm.Page.getAttribute("statuscode").getValue();
    var daduyet = Xrm.Page.getAttribute("new_daduyet").getValue();
    if (trangthai == 100000000 && daduyet == 0) { // da duyệt
        Xrm.Page.getAttribute("new_daduyet").setValue(1);
        var nttrongmiaID = Xrm.Page.data.entity.getId();
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
        xml.push("<entity name='new_chitietnghiemthutrongmia'> ");
        xml.push("<attribute name='new_chitietnghiemthutrongmiaid' /> ");
        xml.push("<attribute name='new_sotienconlaihl' /> ");
        xml.push("<attribute name='new_sotienconlaikhl' /> ");
        xml.push("<attribute name='new_tongchihl' /> ");
        xml.push("<attribute name='new_tongchikhl' /> ");
        xml.push("<filter type='and'> ");
        xml.push("<condition attribute='new_nghiemthutrongmia' operator='eq' value='" + nttrongmiaID + "' /> ");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), true).then(function (rs) {
            var i = 0;
            rs.forEach(function (r) {
                var parameter = {};
                parameter["regarding_Id"] = r.attributes.new_chitietnghiemthutrongmiaid.value;
                parameter["type_parameter"] = 1; // pass parameter : chi tiet nghiem thu trong mia

                var a = r.attributes.new_sotienconlaihl.value;
                var b = r.attributes.new_sotienconlaikhl.value;
                var c = r.attributes.new_tongchihl.value;
                var d = r.attributes.new_tongchikhl.value;
                if (b < d) {
                    parameters.push(parameter);
                    Xrm.Page.ui.setFormHtmlNotification("<strong>Bạn có muốn tạo phiếu điều chỉnh công nợ không ?</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Không' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"" + i + "\");'>Không</a>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Có' href='javascript:' onclick='createphieudieuchinh(" + i + ");'>Có</a>", "WARNING", i + "");
                    i++;
                }
            });
        },
        function (er) {
            console.log(er.message)
        });
    }
}
var parameters = [];
function createphieudieuchinh(i) {
    Xrm.Utility.openEntityForm("new_phieudieuchinhcongno", null, parameters[i]);
}
