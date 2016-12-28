function filter_thuadat_hopdong(isOnload) {
    var hd = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
    var td = Xrm.Page.getAttribute("new_thuadat");
    if (hd != null && hd.length > 0) {
        if (isOnload != true) {
            td.setValue(null);
            td.fireOnChange();
        }
        var viewId = "{1FF0AD7A-7E87-E511-93F3-000C29197875}";
        var entityName = "new_thuadat";
        var viewDisplayName = "Thửa Đất Lookup View";

        var fetchXml = "<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>" +
        "<entity name='new_thuadat'>" +
        "<attribute name='new_thuadatid' />" +
        "<attribute name='new_name' />" +
        "<attribute name='createdon' />" +
        "<link-entity name='new_thuadatcanhtac' from ='new_thuadat' to ='new_thuadatid'>" +
        "<link-entity name='new_nghiemthuchatsatgoc' from='new_hopdongdautumia' to='new_hopdongdautumia'>" +
        "<filter type='and'>" +
        "<condition attribute='new_hopdongdautumia' operator='eq' value ='" + hd[0].id + "' >" +
        "</condition>" +
        "</filter>" +
        "</link-entity>" +
        "</link-entity>" +
        "</entity>" +
        "</fetch>"

        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                            "<row name='result'  " + "id='new_thuadatid'>  " +
                            "<cell name='new_name'   " + "width='200' />  " +
                            "<cell name='createdon'    " + "width='200' />  " +
                            "</row>   " +
                         "</grid>   ";

        Xrm.Page.getControl("new_thuadat").addCustomView(viewId, entityName, viewDisplayName, fetchXml, layoutXml, true);
    } else {
        if (isOnload != true) {
            td.setValue(null);
            td.fireOnChange();
        }
    }
}

function getLenhdoncuoi() {
    var thuadat = Xrm.Page.getAttribute("new_thuadat").getValue();
    var lenhdoncuoi = Xrm.Page.getAttribute("new_lenhdoncuoi");
    var ngaylenhdoncuoi = Xrm.Page.getAttribute("new_ngaylenhdoncuoi");
    var songay = Xrm.Page.getAttribute("new_songaytinhtoihientai");

    if (thuadat != null && thuadat.length > 0) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
        xml.push("<entity name='new_thuadatcanhtac'> ");
        xml.push("<attribute name='new_thuadatcanhtacid' /> ");
        xml.push("<attribute name='new_name' /> ");
        xml.push("<filter type='and'> ");
        xml.push("<condition attribute='new_thuadat' operator='eq' value='" + thuadat[0].id + "' /> ");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), true).then(function (rs) {
            if (rs.length > 0) {
                var xml1 = [];
                xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
                xml1.push("<entity name='new_lenhdon'> ");
                xml1.push("<attribute name='new_name' /> ");
                xml1.push("<attribute name='new_lenhdonid' /> ");
                xml1.push("<attribute name='new_ngaycap' /> ");
                xml1.push("<filter type='and'> ");
                xml1.push("<condition attribute='new_thuacanhtac' operator='eq' value='" + rs[1].attributes.new_thuadatcanhtacid.value + "' /> ");
                xml1.push("</filter>");
                xml1.push("</entity>");
                xml1.push("</fetch>");
                CrmFetchKit.Fetch(xml1.join(""), true).then(function (rs) {
                    if (rs.length > 0) {
                        lenhdoncuoi.setValue([{
                            name: rs[0].attributes.new_name.value,
                            id: rs[0].attributes.new_lenhdonid.value,
                            type: 10086,
                        }]);
                        ngaylenhdoncuoi.setValue(rs[0].attributes.new_ngaycap.value);
                        var ngaycap = new Date(rs[0].attributes.new_ngaycap.value);
                        var ngayhientai = new Date();
                        var diff = Math.abs(ngayhientai - ngaycap) / (1000 * 3600 * 24);
                        songay.setValue(Math.floor(diff));
                    }
                    else
                        Xrm.Page.ui.setFormHtmlNotification("<strong>Thửa đất chưa có lệnh đốn cuối !</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"2\")'> Ẩn thông báo</a>", "WARNING", "2");
                },
                function (er) {
                    console.log(er.message)
                });
            }
        },
        function (er) {
            console.log(er.message)
        });
    }
}
function alertThongbao_Songaychophepdahethan() {
    var timer = null;
    function run() {
        var sn = Xrm.Page.getAttribute("new_songaytinhtoihientai");
        var snc = Xrm.Page.getAttribute("new_songaychophep");
        if (sn != null && sn.getValue() != null && snc != null && snc.getValue() != null) {
            if (timer != null) {
                clearTimeout(timer);
                timer = null;
            }
            if (Xrm.Page.getAttribute("new_songaytinhtoihientai").getValue() - Xrm.Page.getAttribute("new_songaychophep").getValue() > 0) {
                Xrm.Page.ui.setFormHtmlNotification("<strong>Số ngày tính tới hiện tại đã vượt quá số ngày cho phép!</strong>&nbsp;&nbsp;<a class='ms-crm-List-Message-Link-Lite' title='Ẩn thông báo' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"4\");'>[X]</a>", "WARNING", "4");
            }
        }
        else
            timer = setTimeout(run, 1000);
    }
    run();

}
function datten_chitietnghiemthusauthuhoach(IsOnChange) {
    if (Xrm.Page.ui.getFormType() == 1 || IsOnChange == true) {
        debugger;
        var tieude = "CTNTSTH";

        var ntsth = Xrm.Page.getAttribute("new_nghiemthusauthuhoach").getValue();
        if (ntsth != null) {
            var xml = [];
            var hddv;
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml.push("<entity name='new_nghiemthuchatsatgoc'>");
            xml.push("<attribute name='activityid' />");
            xml.push("<attribute name='new_hopdongdautumia' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='activityid' operator='eq' uitype='new_nghiemthuchatsatgoc' value='" + ntsth[0].id + "' />");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.new_hopdongdautumia != null)
                        hddv = rs[0].attributes.new_hopdongdautumia.guid;
                }
            },
                   function (er) {
                       console.log(er.message)
                   });
            if (hddv != null) {
                var xml = [];
                xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
                xml.push("<entity name='new_hopdongdautumia'>");
                xml.push("<attribute name='new_hopdongdautumiaid' />");
                xml.push("<attribute name='new_masohopdong' />");
                xml.push("<filter type='and'>");
                xml.push("<condition attribute='new_hopdongdautumiaid' operator='eq'  uitype='new_hopdongdautumia' value='" + hddv + "'/>");
                xml.push("</filter>");
                xml.push("</entity>");
                xml.push("</fetch>");
                CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                    if (rs.length > 0 && rs[0].attributes.new_masohopdong != null) {
                        maHDDTM = rs[0].attributes.new_masohopdong.value;
                        if (maHDDTM != null)
                            tieude += "-" + maHDDTM;
                    }
                },
                 function (er) {
                     console.log(er.message)
                 });
            }

            var tdArr = ntsth[0].name.split("-");
            if (tdArr.length >= 5) {
                tieude += "-" + tdArr[4];
            }
            var xml2 = [];
            xml2.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml2.push("<entity name='new_chitietnghiemthusauthuhoach'>");
            xml2.push("<attribute name='new_chitietnghiemthusauthuhoachid' alias='sumCT' aggregate='count' />");
            xml2.push("<filter type='and'>");
            xml2.push("<condition attribute='new_nghiemthusauthuhoach' operator='eq' uitype='new_nghiemthuchatsatgoc' value='" + ntsth[0].id + "'/>");
            xml2.push("</filter>");
            xml2.push("</entity>");
            xml2.push("</fetch>");
            CrmFetchKit.Fetch(xml2.join(""), false).then(function (rs2) {
                if (rs2.length > 0 && rs2[0].attributes.sumCT != null) {
                    tieude += "-CT" + (rs2[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        Xrm.Page.getAttribute("new_name").setValue(tieude);
    }
}

function tylemiachay() {
        var miachay = Xrm.Page.getAttribute("new_miachay").getValue();
        var miatuoi = Xrm.Page.getAttribute("new_miatuoi").getValue();
        var tyle = (miachay / (miachay + miatuoi)) * 100;
        Xrm.Page.getControl("new_tylemiachay").setDisabled(false);
        Xrm.Page.getAttribute("new_tylemiachay").setValue(tyle);
        Xrm.Page.getControl("new_tylemiachay").setDisabled(true);
}
