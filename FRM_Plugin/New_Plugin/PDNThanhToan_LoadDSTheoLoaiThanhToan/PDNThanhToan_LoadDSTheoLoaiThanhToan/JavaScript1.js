function filter_trongCTPTT(IsOnChange) {
    debugger;
    var pdntt = Xrm.Page.getAttribute("new_phieudenghithanhtoan").getValue();
    var hd;
    if (pdntt != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml.push("<entity name='new_phieudenghithanhtoan'>");
        xml.push("<attribute name='new_loaithanhtoan' />");
        xml.push("<attribute name='new_hopdongdautumia' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_phieudenghithanhtoanid' operator='eq' uitype='new_phieudenghithanhtoan' value='" + pdntt[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0) {

                if (rs[0].attributes.new_loaithanhtoan != null) {
                    Xrm.Page.getAttribute("new_loaithanhtoan").setValue(rs[0].attributes.new_loaithanhtoan.value);
                }
                if (rs[0].attributes.new_hopdongdautumia != null)
                    hd = rs[0].attributes.new_hopdongdautumia.guid;
            }
        },
               function (er) {
                   console.log(er.message)
               });
        var loaitt = Xrm.Page.getAttribute("new_loaithanhtoan").getValue();
        if (loaitt != null && loaitt == 100000000) {
            var xml = [];
            var viewId = "{4cf9c400-9820-e611-93f7-9abe942a7e29}";
            var entityName = "new_nghiemthudichvu";
            var viewDisplayName = "Nghiệm thu dịch vụ Lookup View";
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
            xml.push("<entity name='new_nghiemthudichvu'>");
            xml.push("<attribute name='activityid'/>");
            xml.push("<attribute name='subject' />");
            xml.push("<attribute name='createdon' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
            xml.push("<condition attribute='new_dathanhtoan' operator='eq' value='0' />");
            xml.push("</filter>");
            xml.push("<order attribute='subject' descending='false' />");
            xml.push("<link-entity name='new_hopdongcungungdichvu' from='new_hopdongcungungdichvuid' to='new_hopdongcungungdichvu' alias='ag'>");
            xml.push("<link-entity name='new_phieudenghithanhtoan' from='new_hopdongcungcapdichvu' to='new_hopdongcungungdichvuid' alias='ah'>");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_phieudenghithanhtoanid' operator='eq' uitype='new_phieudenghithanhtoan'  value='" + pdntt[0].id + "' />");
            xml.push("</filter>");
            xml.push("</link-entity>");
            xml.push("</link-entity>");
            xml.push("</entity>");
            xml.push("</fetch>");
            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='activityid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                        "<row name='result'  " + "id='activityid'>  " +
                                        "<cell name='subject'   " + "width='200' />  " +
                                        "<cell name='createdon'    " + "width='200' />  " +
                                        "</row>   " +
                                     "</grid>   ";
            Xrm.Page.getControl("new_nghiemthudichvu").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);
        }
        else if (loaitt != null && loaitt == 100000001) {
            var xml = [];
            var viewId = "{63DEFD97-53EB-E511-93F6-9ABE942A7E29}";
            var entityName = "new_nghiemthucongtrinh";
            var viewDisplayName = "Nghiệm thu hạ tầng Lookup View";
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
            xml.push("<entity name='new_nghiemthucongtrinh'>");
            xml.push("<attribute name='new_nghiemthucongtrinhid'/>");
            xml.push("<attribute name='new_name' />");
            xml.push("<attribute name='createdon' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
            xml.push("<condition attribute='new_dathanhtoan' operator='eq' value='0' />");
            xml.push("</filter>");
            xml.push("<order attribute='new_name' descending='false' />");
            xml.push("<link-entity name='new_hopdongdautuhatang' from='new_hopdongdautuhatangid' to='new_hopdongdautuhatang' alias='ae'>");
            xml.push("<link-entity name='new_phieudenghithanhtoan' from='new_hopdongdautuhatang' to='new_hopdongdautuhatangid' alias='af'>");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_phieudenghithanhtoanid' operator='eq' uitype='new_phieudenghithanhtoan'  value='" + pdntt[0].id + "' />");
            xml.push("</filter>");
            xml.push("</link-entity>");
            xml.push("</link-entity>");
            xml.push("</entity>");
            xml.push("</fetch>");
            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_nghiemthucongtrinhid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                        "<row name='result'  " + "id='new_nghiemthucongtrinhid'>  " +
                                        "<cell name='new_name'   " + "width='200' />  " +
                                        "<cell name='createdon'    " + "width='200' />  " +
                                        "</row>   " +
                                     "</grid>   ";
            Xrm.Page.getControl("new_nghiemthucongtrinh").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);
        }
        else if (loaitt != null && loaitt == 100000002) {
            var xml = [];
            var kh, khdn, vu;
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml.push("<entity name='new_phieudenghithanhtoan'>");
            xml.push("<attribute name='new_phieudenghithanhtoanid' />");
            xml.push("<attribute name='new_khachhangdoanhnghiep' />");
            xml.push("<attribute name='new_khachhang' />");
            xml.push("<attribute name='new_vudautu' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_phieudenghithanhtoanid' operator='eq' uitype='new_phieudenghithanhtoan' value='" + pdntt[0].id + "' />");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.vu != null) {
                        vu = rs[0].attributes.vu.guid;
                    }
                    if (rs[0].attributes.new_khachhang != null) {
                        kh = rs[0].attributes.new_khachhang.guid;
                    }
                    else if (rs[0].attributes.new_khachhangdoanhnghiep != null) {
                        khdn = rs[0].attributes.new_khachhangdoanhnghiep.guid;
                    }
                }
            },
                   function (er) {
                       console.log(er.message)
                   });
            var xml1 = [];
            var viewId = "{188F3554-4A20-E611-93F7-9ABE942A7E29}";
            var entityName = "new_phieutamung";
            var viewDisplayName = "Phiếu tạm ứng Lookup View";
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' />");
            xml1.push("<attribute name='new_name' />");
            xml1.push("<attribute name='createdon' />");
            xml1.push("<order attribute='new_name' descending='false' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
            if (vu != null)
                xml1.push("<condition attribute='new_vudautu' operator='eq' value='" + vu + "' /> ");
            if (kh != null)
                xml1.push("<condition attribute='new_khachhang' operator='eq' value='" + kh + "' />");
            else if (khdn != null)
                xml1.push("<condition attribute='new_khachhangdoanhnghiep' operator='eq' value='" + khdn + "' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_phieutamungid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                           "<row name='result'  " + "id='new_phieutamungid'>  " +
                                           "<cell name='new_name'   " + "width='200' />  " +
                                           "<cell name='createdon'    " + "width='200' />  " +
                                           "</row>   " +
                                        "</grid>   ";
            Xrm.Page.getControl("new_phieudenghitamung").addCustomView(viewId, entityName, viewDisplayName, xml1.join(""), layoutXml, true);
        }
        else if (loaitt != null && loaitt == 100000003) {

            var xml1 = [];
            var viewId = "{AB3C76DA-AB24-E611-93F7-9ABE942A7E29}";
            var entityName = "new_phieugiaonhanhomgiong";
            var viewDisplayName = "Phiếu giao nhận hom giống Lookup View";
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_phieugiaonhanhomgiong'>");
            xml1.push("<attribute name='new_phieugiaonhanhomgiongid' />");
            xml1.push("<attribute name='new_name' />");
            xml1.push("<attribute name='createdon' />");
            xml1.push("<attribute name='new_masophieu' />");
            xml1.push("<attribute name='new_maphieugiaonhancu' />");
            xml1.push("<order attribute='new_name' descending='false' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongdautumia_doitac' operator='eq' value='" + hd + "' />");
            xml1.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
            xml1.push("<condition attribute='new_loaigiaonhanhom' operator='eq' value='100000001' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_phieugiaonhanhomgiongid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                            "<row name='result'  " + "id='new_phieugiaonhanhomgiongid'>  " +
                                            "<cell name='new_masophieu'   " + "width='200' />  " +
                                            "<cell name='new_maphieugiaonhancu'   " + "width='200' />  " +
                                            "<cell name='new_name'   " + "width='200' />  " +
                                            "<cell name='createdon'    " + "width='200' />  " +
                                            "</row>   " +
                                         "</grid>   ";
            Xrm.Page.getControl("new_phieugiaonhanhomgiong").addCustomView(viewId, entityName, viewDisplayName, xml1.join(""), layoutXml, true);
        }
        if (IsOnChange == true) {
            Xrm.Page.getAttribute("new_nghiemthudichvu").setValue(null);
            Xrm.Page.getAttribute("new_nghiemthucongtrinh").setValue(null);
            Xrm.Page.getAttribute("new_phieugiaonhanhomgiong").setValue(null);
            Xrm.Page.getAttribute("new_phieudenghitamung").setValue(null);
            Xrm.Page.getAttribute("new_tongtien").setValue(null);
        }
    }
}
function loadTongTien() {
    var loaitt = Xrm.Page.getAttribute("new_loaithanhtoan").getValue();
    if (loaitt != null && loaitt == 100000000) {
        var pgn = Xrm.Page.getAttribute("new_nghiemthudichvu").getValue();
        if (pgn != null) {
            var xml1 = [];
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_nghiemthudichvu'>");
            xml1.push("<attribute name='new_tienthanhtoan' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='activityid' operator='eq' value='" + pgn[0].id + "' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.new_tienthanhtoan != null)
                        Xrm.Page.getAttribute("new_tongtien").setValue(rs[0].attributes.new_tienthanhtoan.value);
                    else
                        Xrm.Page.getAttribute("new_tongtien").setValue(0);
                }
            },
                   function (er) {
                       console.log(er.message)
                   });
        }
        else {
            Xrm.Page.getAttribute("new_tongtien").setValue(0);
        }
    }
    else if (loaitt != null && loaitt == 100000001) {
        var pgn = Xrm.Page.getAttribute("new_nghiemthucongtrinh").getValue();
        if (pgn != null) {
            var xml1 = [];
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_nghiemthucongtrinh'>");
            xml1.push("<attribute name='new_nghiemthucongtrinhid' />");
            xml1.push("<attribute name='new_tongtien' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_nghiemthucongtrinhid' operator='eq' value='" + pgn[0].id + "' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.new_tongtien != null) {
                        Xrm.Page.getAttribute("new_tongtien").setValue(rs[0].attributes.new_tongtien.value);
                        load_sotien();
                    }
                    else
                        Xrm.Page.getAttribute("new_tongtien").setValue(0);
                    Xrm.Page.getAttribute("new_tongtien").setSubmitMode("always");
                }
            },
                   function (er) {
                       console.log(er.message)
                   });
        }
        else {
            Xrm.Page.getAttribute("new_tongtien").setValue(0);
        }
    }
    else if (loaitt != null && loaitt == 100000002) {
        var ptu = Xrm.Page.getAttribute("new_phieudenghitamung").getValue();
        if (ptu != null) {
            var xml1 = [];
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' />");
            xml1.push("<attribute name='new_sotienung' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_phieutamungid' operator='eq' value='" + ptu[0].id + "' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.new_sotienung != null)
                        Xrm.Page.getAttribute("new_tongtien").setValue(rs[0].attributes.new_sotienung.value);
                    else
                        Xrm.Page.getAttribute("new_tongtien").setValue(0);
                }
            },
                   function (er) {
                       console.log(er.message)
                   });
        }
    }
    else if (loaitt != null && loaitt == 100000003) {
        var pgn = Xrm.Page.getAttribute("new_phieugiaonhanhomgiong").getValue();
        if (pgn != null) {
            var xml1 = [];
            xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml1.push("<entity name='new_phieugiaonhanhomgiong'>");
            xml1.push("<attribute name='new_phieugiaonhanhomgiongid' />");
            xml1.push("<attribute name='new_tongtien' />");
            xml1.push("<attribute name='new_denghi_khonghoanlai' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_phieugiaonhanhomgiongid' operator='eq' value='" + pgn[0].id + "' />");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0) {
                    if (rs[0].attributes.new_tongtien != null)
                        Xrm.Page.getAttribute("new_tongtien").setValue(rs[0].attributes.new_tongtien.value);
                    else
                        Xrm.Page.getAttribute("new_tongtien").setValue(0);

                    if (rs[0].attributes.new_denghi_khonghoanlai != null)
                        Xrm.Page.getAttribute("new_hotro").setValue(rs[0].attributes.new_denghi_khonghoanlai.value);
                    else
                        Xrm.Page.getAttribute("new_hotro").setValue(0);
                }
            },
                   function (er) {
                       console.log(er.message)
                   });
        }
        else {
            Xrm.Page.getAttribute("new_tongtien").setValue(0);
            Xrm.Page.getAttribute("new_hotro").setValue(0);
        }
    }
}
function setAnhienloaithanhtoan() {
    debugger;
    var loaithanhtoan = Xrm.Page.getAttribute("new_loaithanhtoan").getValue();
    var dichvu = Xrm.Page.getControl("new_nghiemthudichvu");
    var congtrinh = Xrm.Page.getControl("new_nghiemthucongtrinh");
    var homgiong = Xrm.Page.getControl("new_phieugiaonhanhomgiong");
    var tamung = Xrm.Page.getControl("new_phieudenghitamung");

    var dichvus = Xrm.Page.getAttribute("new_nghiemthudichvu");
    var congtrinhs = Xrm.Page.getAttribute("new_nghiemthucongtrinh");
    var homgiongs = Xrm.Page.getAttribute("new_phieugiaonhanhomgiong");
    var tamungs = Xrm.Page.getAttribute("new_phieudenghitamung");
    var hotro = Xrm.Page.getControl("new_hotro");
    var nongdan = Xrm.Page.getControl("new_nongdantratienmat");
    var tongtien = Xrm.Page.getControl("new_tongtien");

    hotro.setVisible(loaithanhtoan == 100000003 || loaithanhtoan == 100000001);
    nongdan.setVisible(loaithanhtoan == 100000001);
    if (loaithanhtoan == 100000000) { //dich vu
        hotro.getAttribute().setRequiredLevel("none");
        nongdan.getAttribute().setRequiredLevel("none");
        tongtien.setDisabled(false);
        hotro.setDisabled(false);
        dichvu.setVisible(true);
        dichvu.setRequiredLevel("required");
        congtrinh.setVisible(false);
        congtrinh.setRequiredLevel("none");
        congtrinhs.setValue(null);
        homgiong.setVisible(false);
        homgiong.setRequiredLevel("none");
        homgiongs.setValue(null);
        tamung.setVisible(false);
        tamung.setRequiredLevel("none");
        tamungs.setValue(null);
    } else if (loaithanhtoan == 100000001) { //ha tang
        hotro.getAttribute().setRequiredLevel("required");
        nongdan.getAttribute().setRequiredLevel("required");
        tongtien.setDisabled(true);
        hotro.setDisabled(true);
        dichvu.setVisible(false);
        dichvu.setRequiredLevel("none");
        dichvus.setValue(null);
        congtrinh.setVisible(true);
        congtrinh.setRequiredLevel("required");
        homgiong.setVisible(false);
        homgiong.setRequiredLevel("none");
        homgiongs.setValue(null);
        tamung.setVisible(false);
        tamung.setRequiredLevel("none");
        tamungs.setValue(null);
    } else if (loaithanhtoan == 100000002) { //tam ung
        hotro.getAttribute().setRequiredLevel("none");
        nongdan.getAttribute().setRequiredLevel("none");
        tongtien.setDisabled(false);
        hotro.setDisabled(false);
        dichvu.setVisible(false);
        dichvu.setRequiredLevel("none");
        dichvus.setValue(null);
        congtrinh.setVisible(false);
        congtrinh.setRequiredLevel("none");
        congtrinhs.setValue(null);
        homgiong.setVisible(false);
        homgiong.setRequiredLevel("none");
        homgiongs.setValue(null);
        tamung.setVisible(true);
        tamung.setRequiredLevel("required");
    } else if (loaithanhtoan == 100000003) { //hom giong
        hotro.getAttribute().setRequiredLevel("none");
        nongdan.getAttribute().setRequiredLevel("none");
        tongtien.setDisabled(false);
        hotro.setDisabled(false);
        dichvu.setVisible(false);
        dichvus.setValue(null);
        dichvu.setRequiredLevel("none");
        congtrinh.setVisible(false);
        congtrinh.setRequiredLevel("none");
        congtrinhs.setValue(null);
        homgiong.setVisible(true);
        homgiong.setRequiredLevel("required");
        tamung.setVisible(false);
        tamung.setRequiredLevel("none");
        tamungs.setValue(null);
    }
}
function filterNghiemThuDichVu(isOnChange) {
    var pdntt = Xrm.Page.getAttribute("new_phieudenghithanhtoan").getValue();
    var ntdv = Xrm.Page.getAttribute("new_nghiemthudichvu");
    if (pdntt != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml.push("<entity name='new_phieudenghithanhtoan'>");
        xml.push("<attribute name='new_khachhangdoanhnghiep' />");
        xml.push("<attribute name='new_khachhang' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_phieudenghithanhtoanid' operator='eq'  value='" + pdntt[0].id + "'/>");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (data) {
            if (data.length > 0) {
                var idkh = null;
                var idkhdn = null;
                if (data[0].attributes.new_khachhang != undefined) {
                    idkh = data[0].attributes.new_khachhang.guid;
                } else if (data[0].attributes.new_khachhangdoanhnghiep != undefined) {
                    idkhdn = data[0].attributes.new_khachhangdoanhnghiep.guid;
                }
                var viewId = "{4B13E5F9-7983-E511-93F3-000C29197875}";
                var entityName = "new_nghiemthudichvu";
                var viewDisplayName = "Nghiệm thu dịch vụ Lookup View";
                var xml1 = [];
                xml1.push("<fetch mapping='logical' version='1.0'>");
                xml1.push("<entity name='new_nghiemthudichvu'>");
                xml1.push("<attribute name='activityid'/>");
                xml1.push("<attribute name='subject'/>");
                xml1.push("<attribute name='createdon'/>");
                xml1.push("<filter type='and'>");
                if (idkh != null) {
                    xml1.push("<condition attribute='new_khachhangdautumia' operator='eq' value='" + idkh + "' />");
                } else if (idkhdn != null) {
                    xml1.push("<condition attribute='new_khachhangdoanhnghiepdautumia' operator='eq' value='" + idkhdn + "' />");
                }
                xml1.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
                xml1.push("<condition attribute='new_dathanhtoan' operator='eq' value='0' />");
                xml1.push("</filter>");
                xml1.push("</entity>");
                xml1.push("</fetch>");
                var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='activityid'  " + "select='1'  " + "icon='0'  " + "preview='0'>  " +
                                            "<row name='result'  " + "id='activityid'>  " +
                                            "<cell name='subject'   " + "width='200' />  " +
                                            "<cell name='createdon'    " + "width='200' />  " +
                                            "</row>   " +
                                         "</grid>   ";
                Xrm.Page.getControl("new_nghiemthudichvu").addCustomView(viewId, entityName, viewDisplayName, xml1.join(""), layoutXml, true);
            } else {
                var viewId = "{4B13E5F9-7983-E511-93F3-000C29197875}";
                var entityName = "new_nghiemthudichvu";
                var viewDisplayName = "Nghiệm thu dịch vụ Lookup View";
                var xml2 = [];
                xml2.push("<fetch mapping='logical' version='1.0'>");
                xml2.push("<entity name='new_nghiemthudichvu'>");
                xml2.push("<attribute name='activityid'/>");
                xml2.push("<attribute name='subject'/>");
                xml2.push("<attribute name='createdon'/>");
                xml2.push("</entity>");
                xml2.push("</fetch>");
                var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='activityid'  " + "select='1'  " + "icon='0'  " + "preview='0'>  " +
                                            "<row name='result'  " + "id='activityid'>  " +
                                            "<cell name='subject'   " + "width='200' />  " +
                                            "<cell name='createdon'    " + "width='200' />  " +
                                            "</row>   " +
                                         "</grid>   ";
                Xrm.Page.getControl("new_nghiemthudichvu").addCustomView(viewId, entityName, viewDisplayName, xml2.join(""), layoutXml, true);
            }
        },
        function (er) {
            console.log(er.message)
        });
    } else {
        var viewId = "{4B13E5F9-7983-E511-93F3-000C29197875}";
        var entityName = "new_nghiemthudichvu";
        var viewDisplayName = "Nghiệm thu dịch vụ Lookup View";
        var xml3 = [];
        xml3.push("<fetch mapping='logical' version='1.0'>");
        xml3.push("<entity name='new_nghiemthudichvu'>");
        xml3.push("<attribute name='activityid'/>");
        xml3.push("<attribute name='subject'/>");
        xml3.push("<attribute name='createdon'/>");
        xml3.push("</entity>");
        xml3.push("</fetch>");
        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='activityid'  " + "select='1'  " + "icon='0'  " + "preview='0'>  " +
                                    "<row name='result'  " + "id='activityid'>  " +
                                    "<cell name='subject'   " + "width='200' />  " +
                                    "<cell name='createdon'    " + "width='200' />  " +
                                    "</row>   " +
                                 "</grid>   ";
        Xrm.Page.getControl("new_nghiemthudichvu").addCustomView(viewId, entityName, viewDisplayName, xml3.join(""), layoutXml, true);
    }
    if (isOnChange) {
        ntdv.setValue(null);
    }
}
function tinhsotiennongdan() {
    var tongtien = Xrm.Page.getAttribute("new_tongtien").getValue();
    var hotro = Xrm.Page.getAttribute("new_hotro").getValue();
    var nongdan = Xrm.Page.getAttribute("new_nongdantratienmat");
    var loaithanhtoan = Xrm.Page.getAttribute("new_loaithanhtoan").getValue();
    if (tongtien != null && loaithanhtoan == 100000001)
        nongdan.setValue(tongtien - hotro);
}
function tinhsotienhotro() {
    var tongtien = Xrm.Page.getAttribute("new_tongtien").getValue();
    var hotro = Xrm.Page.getAttribute("new_hotro");
    var nongdan = Xrm.Page.getAttribute("new_nongdantratienmat").getValue();
    var loaithanhtoan = Xrm.Page.getAttribute("new_loaithanhtoan").getValue();
    if (tongtien != null && loaithanhtoan == 100000001)
        hotro.setValue(tongtien - nongdan);
}
function load_sotien() {
    var nt = Xrm.Page.getAttribute("new_nghiemthucongtrinh").getValue();
    var tong = Xrm.Page.getAttribute("new_tongtien").getValue();
    var nnc = Xrm.Page.getAttribute("new_nongdantratienmat");
    var hotro = Xrm.Page.getAttribute("new_hotro");
    if (nt != null && tong != null) {
        var xml1 = [];
        xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml1.push("<entity name='new_hopdongdautuhatang'>");
        xml1.push("<attribute name='new_hopdongdautuhatangid' />");
        xml1.push("<attribute name='new_sotiennongdanchiu' />");
        xml1.push("<attribute name='new_sotienhotro' />");
        xml1.push("<attribute name='new_hinhthucdonggop' />");
        xml1.push("<attribute name='new_giatrihopdong' />");
        xml1.push("<order attribute='new_sotiennongdanchiu' descending='false' />");
        xml1.push("<link-entity name='new_nghiemthucongtrinh' from='new_hopdongdautuhatang' to='new_hopdongdautuhatangid' alias='ab'>");
        xml1.push("<filter type='and'>");
        xml1.push("<condition attribute='new_nghiemthucongtrinhid' operator='eq' value='" + nt[0].id + "' />");
        xml1.push("</filter>");
        xml1.push("</link-entity>");
        xml1.push("</entity>");
        xml1.push("</fetch>");
        CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
            if (rs.length > 0) {
                if (rs[0].attributes.new_sotiennongdanchiu != null && rs[0].attributes.new_giatrihopdong != null) {
                    var nongdanchiu = rs[0].attributes.new_sotiennongdanchiu.value;
                    var giatri = rs[0].attributes.new_giatrihopdong.value;
                    var ndchiu = (nongdanchiu * tong) / giatri;
                    nnc.setValue(ndchiu);
                    hotro.setValue(tong - ndchiu);
                } else {
                    nnc.setValue(0);
                    hotro.setValue(0);
                }
            }
        },
                  function (er) {
                      console.log(er.message)
                  });
    } else {
        nnc.setValue(0);
        hotro.setValue(0);
    }
}
