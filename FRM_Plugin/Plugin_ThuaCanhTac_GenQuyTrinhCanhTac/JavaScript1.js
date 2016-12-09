//============================ filter Thua Dat tu Khach Hang va Hop Dong Dau Tu Thue Dat =======================================================================
function filter_thuadat_khachhang() {
    var kh = Xrm.Page.getAttribute("new_khachhang").getValue();
    var td = Xrm.Page.getControl("new_thuadat").getAttribute();
    var dn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
    var arr = [];
    if (kh != null && kh.length > 0) {
        var xml = [];
        xml.push("<fetch mapping='logical' version='1.0'  >");
        xml.push("<entity name='new_thuadat'>");
        xml.push("<attribute name='new_thuadatid'/> ");
        xml.push("<filter type='and'>");
        xml.push(" <condition attribute='new_khachhang' operator='eq' value='" + kh[0].id + "'/>");
        xml.push("</filter> ");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            //if (rs.length <= 0) {
            //Xrm.Page.ui.setFormHtmlNotification("<strong>KhÃ¡ch hÃ ng chÆ°a cÃ³ thá»­a Ä‘áº¥t !Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t cho khÃ¡ch hÃ ng!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "1");
            //Xrm.Page.getControl("new_thuadat").setDisabled(true);
            //} else {
            //Xrm.Page.getControl("new_thuadat").setDisabled(false);
            rs.forEach(function (r) {
                arr.push(r.attributes.new_thuadatid.value)
            });
            //}
        },
        function (er) {
            console.log(er.message)
        });
        var xml1 = [];
        xml1.push("<fetch mapping='logical' version='1.0'  >");
        xml1.push("<entity name='new_thuadat'>");
        xml1.push("<attribute name='new_thuadatid'/>");
        xml1.push("<link-entity name='new_datthue' from='new_thuadat' to='new_thuadatid'>");
        xml1.push("<attribute name='new_thuadat'/>");
        xml1.push("<attribute name='new_hopdongthuedat'/>");
        xml1.push("<link-entity name='new_hopdongthuedat' from='new_hopdongthuedatid' to='new_hopdongthuedat'>");
        xml1.push("<filter type='and'>");
        xml1.push("<condition attribute='new_khachhang' operator='eq' value='" + kh[0].id + "'/>");
        xml1.push("</filter>");
        xml1.push("</link-entity>");
        xml1.push("</link-entity>");
        xml1.push("</entity>");
        xml1.push("</fetch>");
        CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
            //if (rs.length <= 0) {
            //Xrm.Page.ui.setFormHtmlNotification("<strong>Há»£p Ä‘á»“ng thuÃª Ä‘áº¥t cá»§a khÃ¡ch hÃ ng chÆ°a cÃ³ thá»­a Ä‘áº¥t!Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t trong há»£p Ä‘á»“ng thuÃª Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"2\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "2");
            //Xrm.Page.getControl("new_thuadat").setDisabled(true);
            //} else {
            //Xrm.Page.getControl("new_thuadat").setDisabled(false);
            rs.forEach(function (r) {
                var vl = r.attributes.new_thuadatid.value;
                if (arr.indexOf(vl) < 0)
                    arr.push(vl);
            });
            //}

            var viewId = "{4B1CB5BE-5F9D-E511-93F0-98BE942A7CB3}";
            var entityName = "new_thuadat";
            var viewDisplayName = "Thá»­a Ä‘áº¥t Lookup View";
            var fetch = [];
            fetch.push("<fetch mapping='logical' version='1.0'  >");
            fetch.push("<entity name='new_thuadat'>");
            fetch.push("<attribute name='new_thuadatid'/>");
            fetch.push("<attribute name='new_name'/>");
            fetch.push("<filter type='and'>");
            fetch.push("<condition attribute='new_thuadatid' operator='in'>");
            if (arr.length <= 0) {
                Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"3\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "3");
                //Xrm.Page.getControl("new_thuadat").setDisabled(true);
            } else {
                Xrm.Page.getControl("new_thuadat").setDisabled(false);
                arr.forEach(function (e) {
                    fetch.push("<value>" + e + "</value>");
                });
            }
            fetch.push("</condition>");
            fetch.push("</filter>");
            fetch.push("</entity>");
            fetch.push("</fetch>");
            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                "<row name='result'  " + "id='new_thuadatid'>  " +
                                "<cell name='new_name'   " + "width='200' />  " +
                                "<cell name='createdon'    " + "width='200' />  " +
                                "</row>   " +
                             "</grid>   ";
            Xrm.Page.getControl("new_thuadat").addCustomView(viewId, entityName, viewDisplayName, fetch.join(""), layoutXml, true);
        },
        function (er) {
            console.log(er.message)
        });

        var viewId = "{4B1CB5BE-5F9D-E511-93F0-98BE942A7CB3}";
        var entityName = "new_thuadat";
        var viewDisplayName = "Thá»­a Ä‘áº¥t Lookup View";
        var fetch = [];
        fetch.push("<fetch mapping='logical' version='1.0'  >");
        fetch.push("<entity name='new_thuadat'>");
        fetch.push("<attribute name='new_thuadatid'/>");
        fetch.push("<attribute name='new_name'/>");
        fetch.push("<filter type='and'>");
        fetch.push("<condition attribute='new_thuadatid' operator='in'>");
        if (arr.length <= 0) {
            //Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"4\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "4");
            // Xrm.Page.getControl("new_thuadat").setDisabled(true);
        } else {
            Xrm.Page.ui.clearFormNotification("4");
            Xrm.Page.ui.clearFormNotification("7");
            Xrm.Page.getControl("new_thuadat").setDisabled(false);
            arr.forEach(function (e) {
                {
                    fetch.push("<value>" + e + "</value>");
                }
            });
        }
        fetch.push("</condition>");
        fetch.push("</filter>");
        fetch.push("</entity>");
        fetch.push("</fetch>");
        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                            "<row name='result'  " + "id='new_thuadatid'>  " +
                            "<cell name='new_name'   " + "width='200' />  " +
                            "<cell name='createdon'    " + "width='200' />  " +
                            "</row>   " +
                         "</grid>   ";
        Xrm.Page.getControl("new_thuadat").addCustomView(viewId, entityName, viewDisplayName, fetch.join(""), layoutXml, true);
    } else if ((kh == null || kh.length <= 0) && (dn == null || dn.length <= 0)) {
        td.setValue(null);
        td.fireOnChange();
    }
}
//=================================================================================================================================================

function filter_thuadat_doanhnghiep() {
    var dn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
    var td = Xrm.Page.getControl("new_thuadat").getAttribute();
    var cn = Xrm.Page.getAttribute("new_khachhang").getValue();
    var arr = [];
    if (dn != null && dn.length > 0) {
        Xrm.Page.getControl("new_xavien").setVisible(true);
        var xml = [];
        xml.push("<fetch mapping='logical' version='1.0'  >");
        xml.push("<entity name='new_thuadat'>");
        xml.push("<attribute name='new_thuadatid'/> ");
        xml.push("<filter type='and'>");
        xml.push(" <condition attribute='new_khachhangdoanhnghiep' operator='eq' value='" + dn[0].id + "'/>");
        xml.push("</filter> ");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            //if (rs.length <= 0) {
            //Xrm.Page.ui.setFormHtmlNotification("<strong>KhÃ¡ch hÃ ng chÆ°a cÃ³ thá»­a Ä‘áº¥t!Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t cho khÃ¡ch hÃ ng!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"5\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "5");
            //Xrm.Page.getControl("new_thuadat").setDisabled(true);
            //} else {
            //Xrm.Page.getControl("new_thuadat").setDisabled(false);
            rs.forEach(function (r) {
                arr.push(r.attributes.new_thuadatid.value)
            });
            // }
        },

        function (er) {
            console.log(er.message)
        });
        var xml1 = [];
        xml1.push("<fetch mapping='logical' version='1.0'  >");
        xml1.push("<entity name='new_thuadat'>");
        xml1.push("<attribute name='new_thuadatid'/>");
        xml1.push("<link-entity name='new_datthue' from='new_thuadat' to='new_thuadatid'>");
        xml1.push("<attribute name='new_thuadat'/>");
        xml1.push("<attribute name='new_hopdongthuedat'/>");
        xml1.push("<link-entity name='new_hopdongthuedat' from='new_hopdongthuedatid' to='new_hopdongthuedat'>");
        xml1.push("<filter type='and'>");
        xml1.push("<condition attribute='new_khachhangdoanhnghiep' operator='eq' value='" + dn[0].id + "'/>");
        xml1.push("</filter>");
        xml1.push("</link-entity>");
        xml1.push("</link-entity>");
        xml1.push("</entity>");
        xml1.push("</fetch>");
        CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
            //if (rs.length <= 0) {
            // Xrm.Page.ui.setFormHtmlNotification("<strong>Há»£p Ä‘á»“ng thuÃª Ä‘áº¥t cá»§a khÃ¡ch hÃ ng chÆ°a cÃ³ thá»­a Ä‘áº¥t!Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t trong há»£p Ä‘á»“ng thuÃª Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"6\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "6");
            //Xrm.Page.getControl("new_thuadat").setDisabled(true);
            //} else {
            //Xrm.Page.getControl("new_thuadat").setDisabled(false);
            rs.forEach(function (r) {
                var vl = r.attributes.new_thuadatid.value;
                if (arr.indexOf(vl) < 0)
                    arr.push(vl);
            });
            //}
        },
        function (er) {
            console.log(er.message)
        });
        var viewId = "{4B1CB5BE-5F9D-E511-93F0-98BE942A7CB3}";
        var entityName = "new_thuadat";
        var viewDisplayName = "Thửa đất Lookup View";
        var fetch = [];
        fetch.push("<fetch mapping='logical' version='1.0'  >");
        fetch.push("<entity name='new_thuadat'>");
        fetch.push("<attribute name='new_thuadatid'/>");
        fetch.push("<attribute name='new_name'/>");
        fetch.push("<filter type='and'>");
        fetch.push("<condition attribute='new_thuadatid' operator='in'>");
        if (arr.length <= 0) {
            // Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lÃ²ng nháº­p thá»­a Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"7\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "7");
            //Xrm.Page.getControl("new_thuadat").setDisabled(true);
        } else {
            Xrm.Page.ui.clearFormNotification("7");
            Xrm.Page.ui.clearFormNotification("4");
            Xrm.Page.getControl("new_thuadat").setDisabled(false);
            arr.forEach(function (e) {
                {
                    fetch.push("<value>" + e + "</value>");
                }
            });
        }

        fetch.push("</condition>");
        fetch.push("</filter>");
        fetch.push("</entity>");
        fetch.push("</fetch>");
        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                            "<row name='result'  " + "id='new_thuadatid'>  " +
                            "<cell name='new_name'   " + "width='200' />  " +
                            "<cell name='createdon'    " + "width='200' />  " +
                            "</row>   " +
                         "</grid>   ";
        Xrm.Page.getControl("new_thuadat").addCustomView(viewId, entityName, viewDisplayName, fetch.join(""), layoutXml, true);
    } else if ((dn == null || dn.length <= 0) && (cn == null || cn.length <= 0)) {
        Xrm.Page.getControl("new_xavien").setVisible(false);
        td.setValue(null);
        td.fireOnChange();
    }
}
//================================= lay (Lai Suat, Loai Lai Suat, Cach Tinh Lai) tu Chinh Sach Dau Tu ==============================================

function getgiatri() {
    var hd = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
    if (hd != null && hd.length > 0) {
        var xml = [];
        xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
        xml.push("<entity name='new_chinhsachdautu'>");
        xml.push("<attribute name='new_muclaisuatdautu'/>");
        xml.push("<attribute name='new_hinhthucdautu'/>");
        xml.push("<attribute name ='new_loailaisuatcodinhthaydoi'/>");
        xml.push("<attribute name ='new_cachtinhlai'/>");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_loaihopdong' operator='eq' value='100000000'/>  ");
        xml.push("</filter>");
        xml.push("<link-entity name='new_new_chinhsachdautu_new_hopdongdautumia' from='new_chinhsachdautuid' to='new_chinhsachdautuid'>");
        xml.push("<link-entity name='new_hopdongdautumia' from='new_hopdongdautumiaid' to='new_hopdongdautumiaid'>");
        xml.push("<link-entity name='new_thuadatcanhtac' from='new_hopdongdautumia' to='new_hopdongdautumiaid'>");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongdautumia' operator='eq' value='" + hd[0].id + "'/>");
        xml.push("</filter>");
        xml.push("</link-entity>");
        xml.push("</link-entity>");
        xml.push("</link-entity>  ");
        xml.push("</entity> ");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), true).then(function (rs) {
            if (rs.length <= 0) {
                alert("Vui lÃ²ng nháº­p giÃ¡ trá»‹ trong chÃ­nh sÃ¡ch Ä‘áº§u tÆ° cá»§a há»£p Ä‘á»“ng !!!");
                Xrm.Page.getAttribute("new_loailaisuat").setValue(null);
                Xrm.Page.getAttribute("new_laisuat").setValue(null);
                Xrm.Page.getAttribute("new_cachtinhlai").setValue(null);
            } else if (rs.length > 0) {
                if (rs[0].attributes.new_loailaisuatcodinhthaydoi.value == true) {
                    alert("Vui lÃ²ng nháº­p cÃ¡c giÃ¡ trá»‹ lÃ£i suáº¥t trong chÃ­nh sÃ¡ch Ä‘áº§u tÆ° cho há»£p Ä‘á»“ng !!!");
                } else {
                    if (rs[0].attributes.new_hinhthucdautu.value == 100000000) {
                        var option = Xrm.Page.getAttribute("new_loailaisuat").getOptions();
                        for (i = 0 ; i < option.length; i++) {
                            var loails = rs[0].attributes.new_loailaisuatcodinhthaydoi.formattedValue;
                            if (option[i].text == loails) {
                                Xrm.Page.getAttribute("new_loailaisuat").setValue(option[i].value);
                            }
                        }
                        var cachtinhlai = Xrm.Page.getAttribute("new_cachtinhlai").getOptions();
                        for (i = 0 ; i < option.length; i++) {
                            var cachtinh = rs[0].attributes.new_cachtinhlai.formattedValue;
                            if (cachtinhlai[i].text == cachtinh) {
                                Xrm.Page.getAttribute("new_cachtinhlai").setValue(cachtinhlai[i].value);
                            }
                        }
                        Xrm.Page.getAttribute("new_laisuat").setValue((rs[0].attributes.new_muclaisuatdautu.value));
                    }
                    else if (rs[0].attributes.new_hinhthucdautu.value == 100000001) {
                        Xrm.Page.getAttribute("new_loailaisuat").setValue(null);
                        Xrm.Page.getAttribute("new_laisuat").setValue(0);
                        Xrm.Page.getAttribute("new_cachtinhlai").setValue(null);
                    }
                }
            }
        },
        function (er) {
            console.log(er.message)
        });
    } else if (hd == null) {
        Xrm.Page.getAttribute("new_loailaisuat").setValue(null);
        Xrm.Page.getAttribute("new_laisuat").setValue(null);
        Xrm.Page.getAttribute("new_cachtinhlai").setValue(null);
    }
}

function filter_thuadat_chusohuu(t) {

    var td = Xrm.Page.getAttribute("new_thuadat").getValue();
    var mul = Xrm.Page.getAttribute("new_multilookupcsh");
    var csh = Xrm.Page.getAttribute("new_chusohuuchinhtd");
    var cshkhdn = Xrm.Page.getAttribute("new_chusohuuchinhtdkhdn");
    var arr = [];
    if (td != null && td.length > 0) {
        var xml = [];
        var viewId = "{1CDB7173-579D-E511-93F0-98BE942A7CB3}";
        var viewDisplayName = "Chá»§ sá»Ÿ há»¯u thá»­a Ä‘áº¥t Lookup View";
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'> ");
        xml.push("<entity name='new_thuadat'> ");
        xml.push("<attribute name='new_chusohuuchinhtd' /> ");
        xml.push("<attribute name='new_chusohuuchinhtdkhdn' /> ");
        xml.push("<filter type='and'> ");
        xml.push("<condition attribute='new_thuadatid' operator='eq' value='" + td[0].id + "' /> ");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length <= 0) {
                Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lÃ²ng nháº­p chá»§ sá»Ÿ há»¯u thá»­a Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "1");
                csh.setValue(null);
                cshkhdn.setValue(null);

            } else if (rs.length > 0) {
                if (rs[0].attributes.new_chusohuuchinhtd == null && rs[0].attributes.new_chusohuuchinhtdkhdn == null) {
                    Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lÃ²ng chá»n chá»§ sá»Ÿ há»¯u  thá»­a Ä‘áº¥t!</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"2\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "2");
                } else {
                    if ((rs[0].attributes.new_chusohuuchinhtd != null)) {
                        mul.setValue([{
                            name: rs[0].attributes.new_chusohuuchinhtd.name,
                            id: rs[0].attributes.new_chusohuuchinhtd.guid,
                            type: 2,
                            typename: "contact"
                        }]);
                        mul.fireOnChange();

                    }
                    else if ((rs[0].attributes.new_chusohuuchinhtdkhdn != null) && (rs[0].attributes.new_chusohuuchinhtd == null)) {
                        mul.setValue([{
                            name: rs[0].attributes.new_chusohuuchinhtdkhdn.name,
                            id: rs[0].attributes.new_chusohuuchinhtdkhdn.guid,
                            type: 1,
                            typename: "account"
                        }]);
                        mul.fireOnChange();
                    }
                }
            }
        },
        function (er) {
            console.log(er.message)
        });
    }
    else if (td == null || td.length <= 0) {
        if (t == true) {
            mul.setValue(null);
        }
    }
}
function setgtrisanluongtheolythuyet(s) {
    var giongmia = Xrm.Page.getAttribute("new_giongmia").getValue();
    var sanluongtheolythuyet = Xrm.Page.getAttribute("new_sanluongtheolythuyet");
    var dientich = Xrm.Page.getAttribute("new_dientichconlai").getValue();

    if (giongmia != null && giongmia.length > 0) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml.push("<entity name='new_giongmia'>");
        xml.push("<attribute name='new_nangsuattiemnangcotuoi' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_giongmiaid' operator='eq' value ='" + giongmia[0].id + "' >");
        xml.push("</condition>");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), true).then(function (rs) {
            if (rs.length <= 0 || rs == null) {
                Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lÃ²ng nháº­p nÄƒng suáº¥t cÃ³ tÆ°á»›i trong giá»‘ng mÃ­a !</strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\")'> áº¨n thÃ´ng bÃ¡o</a>", "WARNING", "1");

            } else if (rs.length > 0 && rs != null) {
                sanluongtheolythuyet.setValue(parseFloat(rs[0].attributes.new_nangsuattiemnangcotuoi.value) * dientich);
            }
        },

    function (er) {
        console.log(er.message)
    });
    }
    else if (giongmia == null || giongmia.length <= 0) {
        if (s == true) {
            sanluongtheolythuyet.setValue(null);
        }
    }
}
function setRequiredKhachHang() {
    var kh = Xrm.Page.getAttribute("new_khachhang").getValue();
    var dn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();
    if (kh == null && dn == null)
        Xrm.Page.getAttribute("new_multilookup").setRequiredLevel("required");

}
function anHienLuuGocTheoLoaiGocMia(isOnChange) {
    var type = Xrm.Page.getAttribute("new_loaigocmia").getValue();
    var lg = Xrm.Page.getControl("new_luugoc");
    var loaitrong = Xrm.Page.getControl("new_loaitrong");
    if (type === 100000001) {
        lg.setVisible(true);
        lg.getAttribute().setRequiredLevel("required");
        loaitrong.setVisible(false);
        loaitrong.getAttribute().setRequiredLevel("none");
    } else {
        lg.setVisible(false);
        lg.getAttribute().setRequiredLevel("none");

        loaitrong.setVisible(true);
        loaitrong.getAttribute().setRequiredLevel("required");
    }
    if (isOnChange) {
        lg.getAttribute().setValue(null);
        loaitrong.getAttribute().setValue(null);
    }
}
function soNamThueDatConLai() {
    var td = Xrm.Page.getAttribute("new_thuadat").getValue();
    var lgm = Xrm.Page.getAttribute("new_loaigocmia").getValue();
    var sncl = Xrm.Page.getAttribute("new_sonamthuedatconlai");
    var hdtd = Xrm.Page.getAttribute("new_hopdongdaututhuedat").getValue();
    var hddtm = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
    var vdthdtd = 0;
    var vdthddtm = 0;
    if (hdtd != null && hdtd.length > 0) {
        var xml = [];
        xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
        xml.push("<entity name='new_hopdongthuedat'>");
        xml.push("<attribute name='new_vudautu'/>");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongthuedatid' operator='eq' value='" + hdtd[0].id + "'>");
        xml.push("</condition>");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), true).then(function (rs) {
            if (rs.length > 0) {
                if (rs[0].attributes.new_vudautu.name != null) {
                    vdthdtd = parseInt(rs[0].attributes.new_vudautu.name.substr(0, 4));
                    if (hddtm != null && hddtm.length > 0) {
                        var xml1 = [];
                        xml1.push("<fetch mapping='logical' version='1.0' distinct='true' >");
                        xml1.push("<entity name='new_hopdongdautumia'>");
                        xml1.push("<attribute name='new_vudautu'/>");
                        xml1.push("<filter type='and'>");
                        xml1.push("<condition attribute='new_hopdongdautumiaid' operator='eq' value='" + hddtm[0].id + "'>");
                        xml1.push("</condition>");
                        xml1.push("</filter>");
                        xml1.push("</entity>");
                        xml1.push("</fetch>");
                        CrmFetchKit.Fetch(xml1.join(""), true).then(function (rs) {
                            if (rs.length > 0) {
                                if (rs[0].attributes.new_vudautu.name != null) {
                                    vdthddtm = parseInt(rs[0].attributes.new_vudautu.name.substr(0, 4));

                                    if (lgm === 100000000 && td != null && td.length > 0) {
                                        var xml2 = [];
                                        xml2.push("<fetch mapping='logical' count='1'  version='1.0' distinct='true' >");
                                        xml2.push("<entity name='new_datthue'>");
                                        xml2.push("<attribute name='new_sonamthuedat'/>");
                                        xml2.push("<filter>");
                                        xml2.push("<condition attribute='new_hopdongthuedat' operator='like' value='" + hdtd[0].id + "'/>");
                                        xml2.push("</filter>");
                                        xml2.push("<link-entity name='new_new_datthue_new_thuadat' from='new_datthueid' to='new_datthueid' alias='dt' link-type='inner' >");
                                        xml2.push("<filter>");
                                        xml2.push("<condition attribute='new_thuadatid' operator='like' value='" + td[0].id + "'/>");
                                        xml2.push("</filter>");
                                        xml2.push("</link-entity>");
                                        xml2.push("</entity>");
                                        xml2.push("</fetch>");
                                        CrmFetchKit.Fetch(xml2.join(""), true).then(function (rs) {
                                            if (rs.length <= 0) {
                                                sncl.setValue(0);
                                            }
                                            else if (rs.length > 0) {
                                                var snt = rs[0].attributes.new_sonamthuedat;
                                                var sntd = 0;
                                                if (snt != undefined)
                                                    sntd = snt.value;
                                                sncl.setValue(vdthdtd + sntd - vdthddtm);
                                            }
                                        },
                                     function (er) {
                                         console.log(er.message)
                                     });
                                    }
                                    else {
                                        sncl.setValue(0);
                                    }
                                }
                            }
                        },
                     function (er) {
                         console.log(er.message)
                     });
                    } else {
                        sncl.setValue(0);
                    }
                }
            }
        },
     function (er) {
         console.log(er.message)
     });
    } else {
        sncl.setValue(0);
    }
}
function setChiPhiDauTu() {
    var tcpdt = Xrm.Page.getAttribute("new_tongchiphidautu").getValue();
    var nstt = Xrm.Page.getAttribute("new_nangsuat").getValue();
    var cpdt = Xrm.Page.getAttribute("new_chiphidautu_tan");
    if (tcpdt != null && nstt != null) {
        cpdt.setValue(tcpdt / nstt);
    } else {
        cpdt.setValue(null);
    }
}

function filter_hopdongthuedat_thuadat(isOnchange) {
    var hdtd = Xrm.Page.getAttribute("new_hopdongdaututhuedat");
    var thuadat = Xrm.Page.getAttribute("new_thuadat").getValue();
    if (thuadat != null && thuadat.length > 0) {
        var xml = [];
        xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
        xml.push("<entity name='new_hopdongthuedat'>");
        xml.push("<attribute name='new_hopdongthuedatid'/>");
        xml.push("<attribute name='new_name' />");
        xml.push("<attribute name='createdon' />");
        xml.push("<link-entity name='new_vudautu' from='new_vudautuid' to='new_vudautu' >");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_danghoatdong' operator='eq' value='1' />");
        xml.push("</filter>");
        xml.push("</link-entity>");
        xml.push("<link-entity name='new_datthue' from='new_hopdongthuedat' to='new_hopdongthuedatid' >");

        xml.push("<link-entity name='new_new_datthue_new_thuadat' from='new_datthueid' to='new_datthueid' >");
        xml.push("<link-entity name='new_thuadat' from='new_thuadatid' to='new_thuadatid' >");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_thuadatid' operator='eq' value='" + thuadat[0].id + "' />");
        xml.push("</filter>");
        xml.push("</link-entity>");
        xml.push("</link-entity>");
        xml.push("</link-entity>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length >= 0) {
                hdtd.setValue([{
                    name: rs[0].attributes.new_name.value,
                    id: rs[0].attributes.new_hopdongthuedatid.value,
                    type: 10073,
                }]);
            }
            else
                hdtd.setValue(null);
        },
        function (er) {
            console.log(er.message)
        });
    }
    else if (thuadat == null || thuadat.length <= 0) {
        if (isOnchange == true) {
            hdtd.setValue(null);
        }
    }
}

function filter_chusohuuchinhTD(s) {
    var chusohuuchinhKH = Xrm.Page.getAttribute("new_chusohuuchinhtd").getValue();
    var chusohuuchinhKHDN = Xrm.Page.getAttribute("new_chusohuuchinhtdkhdn").getValue();

    if (chusohuuchinhKH != null) {
        var thuadat = Xrm.Page.getAttribute("new_thuadat").getValue();
        var loaisohuudat = Xrm.Page.getAttribute("new_loaisohuudat");

        if (thuadat == null) {
            if (s == true) {
                loaisohuudat.setValue(null);
            }
        }

        var kh = Xrm.Page.getAttribute("new_khachhang").getValue();
        var khdn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();

        var hddtthuedat = Xrm.Page.getControl("new_hopdongdaututhuedat");

        if (s == true) {
            if ((kh != null && kh.length > 0) && (chusohuuchinhKHDN != null && chusohuuchinhKHDN.length > 0)) {
                loaisohuudat.setValue(100000000); //dat thue
                hddtthuedat.setVisible(true);
            }
        }

        if (kh != null && chusohuuchinhKH != null) {
            var key = chusohuuchinhKH[0].id.charAt(0);
            if (key == "{") {
                if (kh[0].id == chusohuuchinhKH[0].id) {
                    loaisohuudat.setValue(100000001); //dat nha
                    hddtthuedat.setVisible(false);
                }
                else {
                    loaisohuudat.setValue(100000000); //dat thue
                    hddtthuedat.setVisible(true);
                }
            }
            else {
                var khId = kh[0].id;
                var khSub = khId.substring(1, khId.length - 1);
                khSub = khSub.toLowerCase();

                var chusohuuchinhKHID = chusohuuchinhKH[0].id;

                if (khSub === chusohuuchinhKHID) {
                    loaisohuudat.setValue(100000001); //dat nha
                    hddtthuedat.setVisible(false);
                }
                else {
                    loaisohuudat.setValue(100000000); //dat thue
                    hddtthuedat.setVisible(true);
                }
            }
        }
        else if ((kh == null || kh.length <= 0) && (khdn == null || khdn.length <= 0)) {
            loaisohuudat.setValue(null); //dat thue
            hddtthuedat.setVisible(false);
        }
    }
}

function filter_chusohuuchinhTDKHDN(s) {
    var chusohuuchinhKH = Xrm.Page.getAttribute("new_chusohuuchinhtd").getValue();
    var chusohuuchinhKHDN = Xrm.Page.getAttribute("new_chusohuuchinhtdkhdn").getValue();
    if (chusohuuchinhKHDN != null) {
        var thuadat = Xrm.Page.getAttribute("new_thuadat").getValue();
        var loaisohuudat = Xrm.Page.getAttribute("new_loaisohuudat");

        if (thuadat == null) {
            if (s == true) {
                loaisohuudat.setValue(null);
            }
        }

        var kh = Xrm.Page.getAttribute("new_khachhang").getValue();
        var khdn = Xrm.Page.getAttribute("new_khachhangdoanhnghiep").getValue();

        var hddtthuedat = Xrm.Page.getControl("new_hopdongdaututhuedat");

        if (s == true) {
            if ((khdn != null && khdn.length > 0) && (chusohuuchinhKH != null && chusohuuchinhKH.length > 0)) {
                loaisohuudat.setValue(100000000); //dat thue
                hddtthuedat.setVisible(true);
            }
        }

        if (khdn != null && chusohuuchinhKHDN != null) {
            var key = chusohuuchinhKHDN[0].id.charAt(0);
            if (key == "{") {
                if (khdn[0].id == chusohuuchinhKHDN[0].id) {
                    loaisohuudat.setValue(100000001); //dat nha
                    hddtthuedat.setVisible(false);
                }
                else {
                    loaisohuudat.setValue(100000000); //dat thue
                    hddtthuedat.setVisible(true);
                }
            }
            else {
                var khdnId = khdn[0].id;
                var khdnSub = khdnId.substring(1, khdnId.length - 1);
                khdnSub = khdnSub.toLowerCase();

                if (khdnSub === chusohuuchinhKHDN[0].id) {
                    loaisohuudat.setValue(100000001); //dat nha
                    hddtthuedat.setVisible(false);
                }
                else {
                    loaisohuudat.setValue(100000000); //dat thue
                    hddtthuedat.setVisible(true);
                }
            }

        }
        else if ((kh == null || kh.length <= 0) && (khdn == null || khdn.length <= 0)) {
            loaisohuudat.setValue(null); //dat thue
            hddtthuedat.setVisible(false);
        }
    }
}

function onsave() {
    var formType = Xrm.Page.ui.getFormType();
    var giamiadukien = Xrm.Page.getAttribute("new_giamiadukien");
    var new_dongiadautuhoanlai = Xrm.Page.getAttribute("new_dongiadautuhoanlai");
    var new_dongiadautukhonghoanlai = Xrm.Page.getAttribute("new_dongiadautukhonghoanlai");
    var new_dinhmucdautuhoanlai = Xrm.Page.getAttribute("new_dinhmucdautuhoanlai");
    var new_dinhmucdautukhonghoanlai = Xrm.Page.getAttribute("new_dinhmucdautukhonghoanlai");
    var new_dinhmucdautu = Xrm.Page.getAttribute("new_dinhmucdautu");

    if (formType == 1) { // onload
        giamiadukien.setRequiredLevel("none");
        new_dongiadautuhoanlai.setRequiredLevel("none");
        new_dongiadautukhonghoanlai.setRequiredLevel("none");
        new_dinhmucdautuhoanlai.setRequiredLevel("none");
        new_dinhmucdautukhonghoanlai.setRequiredLevel("none");
        new_dinhmucdautu.setRequiredLevel("none");
    }
    else if (formType == 2) {
        giamiadukien.getValue() == null ? giamiadukien.setRequiredLevel("required") : giamiadukien.setRequiredLevel("none");
        new_dongiadautuhoanlai.getValue() == null ? new_dongiadautuhoanlai.setRequiredLevel("required") : new_dongiadautuhoanlai.setRequiredLevel("none");
        new_dongiadautukhonghoanlai.getValue() == null ? new_dongiadautukhonghoanlai.setRequiredLevel("required") : new_dongiadautukhonghoanlai.setRequiredLevel("none");
        new_dinhmucdautuhoanlai.getValue() == null ? new_dinhmucdautuhoanlai.setRequiredLevel("required") : new_dinhmucdautuhoanlai.setRequiredLevel("none");
        new_dinhmucdautukhonghoanlai.getValue() == null ? new_dinhmucdautukhonghoanlai.setRequiredLevel("required") : new_dinhmucdautukhonghoanlai.setRequiredLevel("none");
        new_dinhmucdautu.getValue() == null ? new_dinhmucdautu.setRequiredLevel("required") : new_dinhmucdautu.setRequiredLevel("none");
    }
}

function setdientich() {
    var thuadat = Xrm.Page.getAttribute("new_thuadat").getValue();
    var dientichhopdong = Xrm.Page.getAttribute("new_dientichhopdong");

    if (thuadat != null && thuadat.length >= 0) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml.push("<entity name='new_thuadat'>");
        xml.push("<attribute name='new_dientich' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_thuadatid' operator='eq' value ='" + thuadat[0].id + "' >");
        xml.push("</condition>");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), true).then(function (rs) {
            if (rs.length > 0 && rs != null) {
                if (dientichhopdong.getValue() == 0 || dientichhopdong.getValue() == null)
                    dientichhopdong.setValue(parseFloat(rs[0].attributes.new_dientich.value));
            }
            else
                dientichhopdong.setValue(0);
        },
        function (er) {
            console.log(er.message)
        });
    }
    else {
        dientichhopdong.setValue(0);
    }
}
function datten_chitiethopdongdautumiaONCHANGE() {

    debugger;
    var tieude = "CTHDDT";
    var hddtm = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
    if (hddtm != null) {
        var xml = [];
        xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
        xml.push("<entity name='new_hopdongdautumia'>");
        xml.push("<attribute name='new_hopdongdautumiaid' />");
        xml.push("<attribute name='new_masohopdong' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongdautumiaid' operator='eq'  uitype='new_hopdongdautumia' value='" + hddtm[0].id + "'/>");
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
    Xrm.Page.getAttribute("new_name").setValue(tieude);
}
function datten_chitiethopdongdautumiaONLOADNEW() {
    if (Xrm.Page.ui.getFormType() == 1) {
        debugger;
        var tieude = "CTHDDT";
        var hddtm = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
        if (hddtm != null) {
            var xml = [];
            xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
            xml.push("<entity name='new_hopdongdautumia'>");
            xml.push("<attribute name='new_hopdongdautumiaid' />");
            xml.push("<attribute name='new_masohopdong' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongdautumiaid' operator='eq'  uitype='new_hopdongdautumia' value='" + hddtm[0].id + "'/>");
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

            var xml1 = [];
            xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml1.push("<entity name='new_thuadatcanhtac'>");
            xml1.push("<attribute name='new_thuadatcanhtacid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='" + hddtm[0].id + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        Xrm.Page.getAttribute("new_name").setValue(tieude);
    }
}
function phuluchd() {
    debugger;
    var formType = Xrm.Page.ui.getFormType();
    var phuluchopdong = Xrm.Page.getAttribute("new_phuluchopdongid");
    var hopdongdautumia = Xrm.Page.getControl("new_hopdongdautumia");

    if (phuluchopdong.getValue() == null) {
        hopdongdautumia.setVisible(true);
        hopdongdautumia.setRequiredLevel("required");
    }
    else {
        hopdongdautumia.setVisible(false);
        hopdongdautumia.setRequiredLevel("none");
    }
}
function phulucdaduyet() {
    var phulucid = Xrm.Page.getAttribute("new_phuluchopdongid").getValue();
    if (phulucid != null) { // da duyet
        var xml1 = [];
        xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml1.push("<entity name='new_phuluchopdong'>");
        xml1.push("<attribute name='new_tinhtrangduyet' />");
        xml1.push("<order attribute='new_name' descending='false' />");
        xml1.push("<filter type='and'>");
        xml1.push("<condition attribute='new_phuluchopdongid' operator='eq' uitype='new_phuluchopdong' value='" + phulucid[0].id + "'/>");
        xml1.push("</filter>");
        xml1.push("</entity>");
        xml1.push("</fetch>");
        CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs != null) {
                hopdongdautumia = Xrm.Page.getControl("new_hopdongdautumia");
                var tinhtrangduyet = rs[0].attributes.new_tinhtrangduyet;
                if (tinhtrangduyet != undefined) {
                    if (tinhtrangduyet.value == "100000005") {
                        hopdongdautumia.setVisible(true);
                        hopdongdautumia.setRequiredLevel("required");
                    }
                }
            }
        },
     function (er) {
         console.log(er.message)
     });
    }
}

function nhacnhophuluchd() {
    var formType = Xrm.Page.ui.getFormType();
    var phuluchopdong = Xrm.Page.getAttribute("new_phuluchopdongid");
    var loaishdat = Xrm.Page.getAttribute("new_loaisohuudat").getValue();
    if (formType == 1 && phuluchopdong.getValue() != null && loaishdat == 100000000) {
        alert("HĐ thuê đất, chi tiết HĐTĐ và chi tiết HĐTĐ thửa đất đã được tạo. Vui lòng nhập đầy đủ thông tin các hợp đồng trên.");
    }
}

function filter_thuadat_khongtontaichitiethopdongdautumia(isOnchange) {
    var hddtm = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
    var td = Xrm.Page.getAttribute("new_thuadat");
    if (hddtm != null) {
        var xml = [];
        var hdtd;
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml.push("<entity name='new_hopdongdautumia'>");
        xml.push("<attribute name='new_hopdongdautumiaid' />");
        xml.push("<attribute name='new_vudautu' />");
        xml.push("<attribute name='new_statuscode' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongdautumiaid' operator='eq' uitype='new_hopdongdautumia' value='" + hddtm[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0) {
                if (rs[0].attributes.new_thuadat != null) {
                    hdtd = rs[0].attributes.new_thuadat.guid;
                }
            }
        },
          function (er) {
              console.log(er.message)
          });

        if (hdtd != null) {
            var xml1 = [];
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_thuadat'>");
            xml1.push("<attribute name='new_name' />");
            xml1.push("<attribute name='createdon' />");
            xml1.push("<attribute name='new_dientich' />");
            xml1.push("<attribute name='new_thuadatid' />");
            xml1.push("<order attribute='new_name' descending='false' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_thuadatid' operator='ne' uitype='new_thuadat' value='" + hdtd + "' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");

            var viewId = "{C1D291E4-2FDC-4607-823B-66DC78D29D0B}";
            var entityName = "new_thuadat";
            var viewDisplayName = "Thửa đất Lookup View";

            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                "<row name='result'  " + "id='new_thuadatid'>  " +
                                "<cell name='new_name'   " + "width='200' />  " +
                                "<cell name='createdon'    " + "width='200' />  " +
                                "</row>   " +
                             "</grid>   ";

            Xrm.Page.getControl("new_thuadat").addCustomView(viewId, entityName, viewDisplayName, xml1.join(""), layoutXml, true);
        }

    }
    if (isOnchange == true) {
        td.setValue(null);
    }
}

function setlydohuy(fieldname) {
    var entity = Xrm.Page.data.entity.getEntityName();

    var xml = [];
    var viewId = "{C195D6AE-43A2-E511-93F0-98BE942A7CB3}";
    var entityName = "new_lydo";
    var viewDisplayName = "Lý do Lookup View";
    xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
    xml.push("<entity name='new_lydo'>");
    xml.push("<attribute name='new_lydoid'/>");
    xml.push("<attribute name='new_name' />");
    xml.push("<attribute name='new_loailydo' />");
    xml.push("<attribute name='new_dinhnghia' />");
    xml.push("<attribute name='createdon' />");
    xml.push("<filter type='and'>");
    xml.push("<condition attribute='new_entityname' operator='eq' value ='" + entity + "' />");
    xml.push("<condition attribute='new_fieldlogical' operator='eq' value ='" + fieldname + "' />");
    xml.push("</filter>");
    xml.push("</entity>");
    xml.push("</fetch>");
    var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_lydoid'  " + "select='1'  " + "icon='0'  " + "preview='0'>  " +
                            "<row name='result'  " + "id='new_lydoid'>  " +
                            "<cell name='new_name'   " + "width='125' />  " +
                            "<cell name='new_loailydo'    " + "width='100' />  " +
                            "<cell name='new_doituong'    " + "width='125' />  " +
                            "<cell name='new_dinhnghia'    " + "width='100' />  " +
                            "<cell name='createdon'    " + "width='125' />  " +
                            "</row>   " +
                         "</grid>   ";
    Xrm.Page.getControl(fieldname).addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);
}

function setloai() {
    var loai = Xrm.Page.getAttribute("new_loai").getValue();

    var dautuhoanlai = Xrm.Page.getControl("new_dautuhoanlai");
    var dautukhonghoanlai = Xrm.Page.getControl("new_dautukhonghoanlai");
    var sotienconlaihl = Xrm.Page.getControl("new_dinhmucdautuhoanlai_hientai_hidden");
    var sotienconlaikhl = Xrm.Page.getControl("new_dinhmucdautukhonghoanlai_hientai_hidden");
    var dachihl = Xrm.Page.getControl("new_dachihl");
    var dachikhl = Xrm.Page.getControl("new_dachikhl");
    var tongchihl = Xrm.Page.getControl("new_tongchihoanlai");
    var tongchikhl = Xrm.Page.getControl("new_tongchikhonghoanlai");
    var sotienchuyentukhlsanghl = Xrm.Page.getControl("new_sotienchuyentukhlthanhhl");
    var dongiahl = Xrm.Page.getControl("new_dinhmucchinhsachhoanlai");
    var dongiakhl = Xrm.Page.getControl("new_dinhmucchinhsachkhonghoanlai");
    var sotienthaydoihl = Xrm.Page.getControl("new_sotienthaydoihl");
    var sotienthaydoikhl = Xrm.Page.getControl("new_sotienthaydoikhl");
    var sotiendieuchinh = Xrm.Page.getControl("new_sotiendieuchinh");

    if (loai == 100000000) { //giam
        dautuhoanlai.setVisible(true);
        dautukhonghoanlai.setVisible(true);
        sotienconlaihl.setVisible(true);
        sotienconlaikhl.setVisible(true);
        tongchihl.setVisible(true);
        tongchikhl.setVisible(true);
        dongiahl.setVisible(true);
        dongiakhl.setVisible(true);
        sotienthaydoihl.setVisible(true);
        sotienthaydoikhl.setVisible(true);
        sotiendieuchinh.setVisible(true);

        dachihl.setVisible(false);
        dachikhl.setVisible(false);
        sotienchuyentukhlsanghl.setVisible(false);
    }
    else if (loai == 100000001) { //huy
        dautuhoanlai.setVisible(false);
        dautukhonghoanlai.setVisible(false);
        sotienconlaihl.setVisible(false);
        sotienconlaikhl.setVisible(false);
        tongchihl.setVisible(false);
        tongchikhl.setVisible(false);
        dongiahl.setVisible(false);
        dongiakhl.setVisible(false);
        sotienthaydoihl.setVisible(false);
        sotienthaydoikhl.setVisible(false);
        sotiendieuchinh.setVisible(false);

        dachihl.setVisible(true);
        dachikhl.setVisible(true);
        sotienchuyentukhlsanghl.setVisible(true);
    }
}


function setTieuDe() {
    if (Xrm.Page.ui.getFormType() == 1) {
        var entityName = $("span#form_title_div:first").find("span.ms-crm-FormSelector:first").text().split(':')[0];
        var newName = Xrm.Page.getAttribute("new_name");
        var today = new Date();
        var dd = today.getDate();
        var mm = today.getMonth() + 1;
        var yyyy = today.getFullYear();
        var today = dd + '/' + mm + '/' + yyyy;
        if (newName.getValue() == null)
            newName.setValue(entityName + " - " + today);
    }
}

function taophieudieuchinhcongno() {
    var trangthai = Xrm.Page.getAttribute("new_trangthai").getValue();
    var loai = Xrm.Page.getAttribute("new_loai").getValue();
    var daduyet = Xrm.Page.getAttribute("new_daduyet").getValue();

    if (trangthai == 100000002 && loai == 100000000 && daduyet == 0) {  // da duyet va loai la giam
        var parameters = {};

        //parameters.new_ngaylapphieu = new Date().toISOString();
        //parameters.new_bienbangiamhuydientich = Xrm.Page.data.entity.getId();
        //parameters.new_bienbangiamhuydientichname = Xrm.Page.getAttribute("new_name").getValue();

        //var lookupFieldhddtm = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
        //var lookupFieldchitiethddtm = Xrm.Page.getAttribute("new_thuadatcanhtac").getValue();
        //parameters.new_hopdongdautumia = lookupFieldhddtm[0].id;
        //parameters.new_hopdongdautumianame = lookupFieldhddtm[0].name;
        //parameters.new_chitiethddtmia = lookupFieldchitiethddtm[0].id;
        //parameters.new_chitiethddtmianame = lookupFieldchitiethddtm[0].name;
        //parameters.new_dinhmuchoanlaibandau = Xrm.Page.getAttribute("new_dautuhoanlai").getValue();
        //parameters.new_dinhmuckhonghoanlaibandau = Xrm.Page.getAttribute("new_dautukhonghoanlai").getValue();
        //parameters.new_tongtienchihlbandau = Xrm.Page.getAttribute("new_tongchihoanlai").getValue();
        //parameters.new_tongtienchikhlbandau = Xrm.Page.getAttribute("new_tongchikhonghoanlai").getValue();

        //parameters.new_dientichthaydoi = "5"; //Xrm.Page.getAttribute("new_dientichgiamhuy");

        //parameters.new_dinhmucchinhsachhoanlai = Xrm.Page.getAttribute("new_dinhmucchinhsachhoanlai").getValue();
        //parameters.new_dinhmucchinhsachkhonghoanlai = Xrm.Page.getAttribute("new_dinhmucchinhsachkhonghoanlai").getValue();
        //parameters.new_sotienthaydoi = Xrm.Page.getAttribute("new_dientichgiamhuy").getValue() * Xrm.Page.getAttribute("new_dinhmucchinhsachhoanlai").getValue();
        //parameters.new_sotienthaydoikhl = Xrm.Page.getAttribute("new_dientichgiamhuy").getValue() * Xrm.Page.getAttribute("new_dinhmucchinhsachkhonghoanlai").getValue();
        parameters["regarding_Id"] = Xrm.Page.data.entity.getId();
        Xrm.Page.getAttribute("new_daduyet").setValue(1);
        var a = Xrm.Page.getAttribute("new_dinhmucdautuhoanlai_hientai_hidden").getValue();
        var b = Xrm.Page.getAttribute("new_dinhmucdautukhonghoanlai_hientai_hidden").getValue();
        var c = Xrm.Page.getAttribute("new_tongchihoanlai").getValue();
        var d = Xrm.Page.getAttribute("new_tongchikhonghoanlai").getValue();
        if (b < d) {
            window.top.$ui.Confirm("Xác nhận", "Bạn có muốn tạo phiếu điều chỉnh công nợ không ?", function (e) {
                //window.parent.Xrm.Utility.openEntityForm("new_phieudieuchinhcongno", null, null, { openInNewWindow: true });
                Xrm.Utility.openEntityForm("new_phieudieuchinhcongno", null, parameters);
            }, null);
        }
    }
}
function datten_chitietbienbanthuhoachsom() {
    debugger;
    var tieute = "CTBBTHS";
    var cthddtm = Xrm.Page.getAttribute("new_chitiethddtmia").getValue();
    if (cthddtm != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml.push("<entity name='new_thuadatcanhtac'>");
        xml.push("<attribute name='new_thuadatcanhtacid' />");
        xml.push("<attribute name='new_khachhangdoanhnghiep'  />");
        xml.push("<attribute name='new_khachhang'/>");
        xml.push("<attribute name='new_thuadat'/>");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_thuadatcanhtacid' operator='eq' uitype='new_thuadatcanhtac' value='" + cthddtm[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0) {
                if (rs[0].attributes.new_khachhang != null)
                    tieute += "-" + rs[0].attributes.new_khachhang.name;
                else if (rs[0].attributes.new_khachhangdoanhnghiep != null)
                    tieute += "-" + rs[0].attributes.new_khachhangdoanhnghiep.name;
                if (rs[0].attributes.new_thuadat != null)
                    tieute += "-" + rs[0].attributes.new_thuadat.name;
            }
        },
         function (er) {
             console.log(er.message)
         });
    }

    Xrm.Page.getAttribute("new_name").setValue(tieute);
}
