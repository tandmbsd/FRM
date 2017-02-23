function setLoaigocmia(isOnChange) {
    var loaigocmia = Xrm.Page.getAttribute("new_loaigocmia").getValue();
    var luugoc = Xrm.Page.getControl("new_luugoc");
    var loaitrong = Xrm.Page.getControl("new_loaitrong");
    var luugocAtt = Xrm.Page.getAttribute("new_luugoc");
    loaitrong.setVisible(loaigocmia == 100000000);
    if (loaigocmia != null && loaigocmia == 100000001) {
        luugoc.setVisible(true);
        luugocAtt.setRequiredLevel('required');
        loaitrong.setRequiredLevel('none');
    } else if (loaigocmia != null && loaigocmia == 100000000) {
        luugoc.setVisible(false);
        luugocAtt.setRequiredLevel('none');
        loaitrong.setRequiredLevel('required');
    }
    if (isOnChange) {
        luugocAtt.setValue(null);
        loaitrong.getAttribute().setValue(null);
    }
}
function tinhtongdautuchiphi() {
    debugger;
    var dthl = Xrm.Page.getAttribute("new_dautuhoanlai").getValue();
    var dtkhl = Xrm.Page.getAttribute("new_dautukhonghoanlai").getValue();
    var tcpdt = Xrm.Page.getAttribute("new_tongchiphidautu");
    tcpdt.setValue(dthl + dtkhl);
    tcpdt.setSubmitMode("always");
}
function setdongiaphanbonhd() {

    var dgpbhd = Xrm.Page.getAttribute("new_dongiaphanbonhd").getValue();
    var dgpbtt = Xrm.Page.getAttribute("new_dongiaphanbontoithieu").getValue();

    if (dgpbhd > dgpbtt) {
        Xrm.Page.ui.setFormNotification("Vui lòng nhập đơn giá phân bón hợp đồng không được lớn hơn đơn giá phân bón tối thiểu", "ERROR", "1");
    } else {
        Xrm.Page.ui.clearFormNotification("1");
    }
}
function sum_dautuhoanlai() {
    var dientich = Xrm.Page.getAttribute("new_dientichhopdong").getValue();
    var dongiahdhoanlai = Xrm.Page.getAttribute("new_dongiahopdong").getValue();
    if (dientich != null && dongiahdhoanlai != null) {
        Xrm.Page.getAttribute("new_dautuhoanlai").setValue(dientich * dongiahdhoanlai);
        Xrm.Page.getAttribute("new_dautuhoanlai").setSubmitMode("always");
        tinhtongdautuchiphi();
    }
    var dongiadthl = Xrm.Page.getAttribute("new_dongiadautuhoanlai").getValue();
    if (dongiahdhoanlai > dongiadthl)
        Xrm.Page.ui.setFormNotification("Vui lòng nhập đơn giá hợp đồng HL không được lớn hơn đơn giá ĐT hoàn lại", "ERROR", "2");
    else
        Xrm.Page.ui.clearFormNotification("2");
}
function sum_dautukhonghoanlai() {
    var dientich = Xrm.Page.getAttribute("new_dientichhopdong").getValue();
    var dongiahdkhoanlai = Xrm.Page.getAttribute("new_dongiahopdongkhl").getValue();
    if (dientich != null && dongiahdkhoanlai != null) {
        Xrm.Page.getAttribute("new_dautukhonghoanlai").setValue(dientich * dongiahdkhoanlai);
        Xrm.Page.getAttribute("new_dautukhonghoanlai").setSubmitMode("always");
        tinhtongdautuchiphi();
    }
    var dongiadtkhl = Xrm.Page.getAttribute("new_dongiadautukhonghoanlai").getValue();
    if (dongiahdkhoanlai > dongiadtkhl)
        Xrm.Page.ui.setFormNotification("Vui lòng nhập đơn giá hợp đồng không HL không được lớn hơn đơn giá ĐT không hoàn lại", "ERROR", "3");
    else
        Xrm.Page.ui.clearFormNotification("3");
}
function filter_thuadat_hddtm(s) {
    var pl = Xrm.Page.getAttribute("new_phuluchopdong").getValue();
    if (pl != null && pl.length > 0) {
        var xml1 = [];
        xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
        xml1.push("<entity name='new_phuluchopdong'> ");
        xml1.push("<attribute name='new_hopdongdautumia' /> ");
        xml1.push("<filter type='and'> ");
        xml1.push("<condition attribute='new_phuluchopdongid' operator='eq' value='" + pl[0].id + "' /> ");
        xml1.push("</filter>");
        xml1.push("</entity>");
        xml1.push("</fetch>");
        CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
            var thuadat = Xrm.Page.getAttribute("new_thuadat");

            if (rs[0].attributes.new_hopdongdautumia != null) {
                var xml = [];
                var viewId = "{73A33CD4-DFC4-E511-93F1-9ABE942A7E29}";
                var entityName = "new_thuadat";
                var viewDisplayName = "Thửa đất Lookup View";
                xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
                xml.push("<entity name='new_thuadat'> ");
                xml.push("<attribute name='new_thuadatid' /> ");
                xml.push("<attribute name='new_dientich' /> ");
                xml.push("<attribute name='new_name' /> ");
                xml.push("<attribute name='createdon' /> ");
                xml.push("<link-entity name='new_thuadatcanhtac' from='new_thuadat' to='new_thuadatid'>");
                xml.push("<link-entity name='new_hopdongdautumia' from='new_hopdongdautumiaid' to='new_hopdongdautumia'>");
                xml.push("<filter type='and'> ");
                xml.push("<condition attribute='new_hopdongdautumiaid' operator='eq' value='" + rs[0].attributes.new_hopdongdautumia.guid + "' /> ");
                xml.push("</filter>");
                xml.push("</link-entity>");
                xml.push("</link-entity>");
                xml.push("</entity>");
                xml.push("</fetch>");
                var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                        "<row name='result'  " + "id='new_thuadatid'>  " +
                                        "<cell name='new_name'   " + "width='200' />  " +
                                        "<cell name='new_dientich'    " + "width='200' />  " +
                                        "<cell name='createdon'    " + "width='200' />  " +
                                        "</row>   " +
                                     "</grid>   ";
                Xrm.Page.getControl("new_thuadat").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);
            }
            else
                thuadat.setValue(null);
        },
        function (er) {
            console.log(er.message)
        });
    }
}
function kiemtra_dientich(isOnChange) {
    var td = Xrm.Page.getAttribute("new_thuadat").getValue();
    var dthd = Xrm.Page.getAttribute("new_dientichhopdong");
    if (td != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
        xml.push("<entity name='new_thuadat'> ");
        xml.push("<attribute name='new_thuadatid' /> ");
        xml.push("<attribute name='new_dientich' /> ");
        xml.push("<filter type='and'> ");
        xml.push("<condition attribute='new_thuadatid' operator='eq' value='" + td[0].id + "' /> ");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (data) {
            if (data.length > 0) {
                var dt = data[0].attributes.new_dientich.value;
                if (dt < dthd.getValue()) {
                    Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lòng nhập diện tích hợp đồng không lớn hơn diện tích thửa đất.</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\")'> Ẩn thông báo</a>", "WARNING", "1");
                    dthd.setValue(null);
                } else {
                    Xrm.Page.ui.clearFormNotification("1");

                }
            }
        },
        function (er) {
            console.log(er.message)
        });
    }
}
function setDienTichHopDong(isOnChange) {
    var td = Xrm.Page.getAttribute("new_thuadat").getValue();

    var loaitrong = Xrm.Page.getAttribute("new_loaitrong");
    var vutrong = Xrm.Page.getAttribute("new_vutrong");
    var loaigocmia = Xrm.Page.getAttribute("new_loaigocmia");
    var tuoimia = Xrm.Page.getAttribute("new_tuoimia");
    var mucdichsanxuat = Xrm.Page.getAttribute("new_mucdichsanxuatmia");
    var giongmia = Xrm.Page.getAttribute("new_giongmiadangky");
    var ngaytrongdukien = Xrm.Page.getAttribute("new_ngaytrongdukien");
    var dientich = Xrm.Page.getAttribute("new_dientichhopdong");
    var nguongocdat = Xrm.Page.getAttribute("new_nguongocdat");
    var sonam = Xrm.Page.getAttribute("new_thoihanthuedatconlai");
    if (Xrm.Page.ui.getFormType() == 1 || isOnChange || dthd.getValue() == null) {
        if (td != null) {
            var xml = [];
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
            xml.push("<entity name='new_thuadatcanhtac'> ");
            xml.push("<attribute name='new_thuadatcanhtacid'/> ");
            xml.push("<attribute name='new_giongmia' /> ");
            xml.push("<attribute name='new_loaigocmia'/> ");
            xml.push("<attribute name='new_loaitrong' /> ");
            xml.push("<attribute name='new_mucdichsanxuatmia' /> ");
            xml.push("<attribute name='new_ngaytrongdukien' /> ");
            xml.push("<attribute name='new_tuoimia' /> ");
            xml.push("<attribute name='new_vutrong' /> ");
            xml.push("<attribute name='new_dientichhopdong' /> ");
            xml.push("<attribute name='new_loaisohuudat' /> ");
            xml.push("<attribute name='new_sonamthuedatconlai' /> ");
            xml.push("<filter type='and'> ");
            xml.push("<condition attribute='new_thuadat' operator='eq' value='" + td[0].id + "' /> ");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (data) {
                if (data.length > 0) {

                    if (data[0].attributes.new_dientichhopdong != undefined) {
                        dientich.setValue(data[0].attributes.new_dientichhopdong.value);
                        sum_dautuhoanlai();
                        sum_dautukhonghoanlai();
                    }
                    if (data[0].attributes.new_loaitrong != null)
                        loaitrong.setValue(data[0].attributes.new_loaitrong.value);
                    if (data[0].attributes.new_vutrong != null)
                        vutrong.setValue(data[0].attributes.new_vutrong.value);
                    if (data[0].attributes.new_loaigocmia != null)
                        loaigocmia.setValue(data[0].attributes.new_loaigocmia.value);
                    if (data[0].attributes.new_tuoimia != null)
                        tuoimia.setValue(data[0].attributes.new_tuoimia.value);
                    if (data[0].attributes.new_mucdichsanxuatmia != null)
                        mucdichsanxuat.setValue(data[0].attributes.new_mucdichsanxuatmia.value);
                    if (data[0].attributes.new_giongmia != null) {
                        giongmia.setValue([{
                            name: data[0].attributes.new_giongmia.name,
                            id: data[0].attributes.new_giongmia.guid,
                            typename: data[0].attributes.new_giongmia.logicalName
                        }]);
                    }
                    if (data[0].attributes.new_ngaytrongdukien != null)
                        ngaytrongdukien.setValue(data[0].attributes.new_ngaytrongdukien.value);
                    if (data[0].attributes.new_loaisohuudat != null)
                        nguongocdat.setValue(data[0].attributes.new_loaisohuudat.value);
                    if (data[0].attributes.new_sonamthuedatconlai != null)
                        sonam.setValue(data[0].attributes.new_sonamthuedatconlai.value);

                }
            },
            function (er) {
                console.log(er.message)
            });
        }
    }
}
function datten_chitietphuluctangdientich(IsOnChange) {
    if (Xrm.Page.ui.getFormType() == 1 || IsOnChange == true) {
        debugger;
        var tieude = "";

        var phuluc = Xrm.Page.getAttribute("new_phuluchopdong").getValue();
        var thuadat = Xrm.Page.getAttribute("new_thuadat").getValue();
        if (phuluc != null) {
            var xml = [];

            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml.push("<entity name='new_phuluchopdong'>");
            xml.push("<attribute name='new_phuluchopdongid' />");
            xml.push("<attribute name='new_name' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_phuluchopdongid' operator='eq' value='" + phuluc[0].id + "' />");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.new_name != null)
                        tieude += rs[0].attributes.new_name.value;

                }
            },
                   function (er) {
                       console.log(er.message)
                   });
            if (thuadat != null) {
                var xml = [];
                xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
                xml.push("<entity name='new_thuadat'>");
                xml.push("<attribute name='new_thuadatid' />");
                xml.push("<attribute name='new_name' />");
                xml.push("<filter type='and'>");
                xml.push("<condition attribute='new_thuadatid' operator='eq' value='" + thuadat[0].id + "'/>");
                xml.push("</filter>");
                xml.push("</entity>");
                xml.push("</fetch>");
                CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                    if (rs.length > 0 && rs[0].attributes.new_name != null) {
                        tieude += "-" + rs[0].attributes.new_name.value;

                    }
                },
                 function (er) {
                     console.log(er.message)
                 });
            }


        }
        Xrm.Page.getAttribute("new_name").setValue(tieude);
    }
}
