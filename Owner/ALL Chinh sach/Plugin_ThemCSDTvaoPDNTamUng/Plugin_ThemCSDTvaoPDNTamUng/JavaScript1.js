function datten_chitietphieudenghitamung() {
    debugger;
    var tieude = "CTDNTU";
    var pdntu = Xrm.Page.getAttribute("new_phieudenghitamung").getValue();
    if (pdntu != null) {
        var hddtm, hddttd, hddtmmtb, hdvc, hdth, loaihd, hatang;
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml.push("<entity name='new_phieutamung'>");
        xml.push("<attribute name='new_phieutamungid' />");
        xml.push("<attribute name='new_hopdongdaututhuedat' />");
        xml.push("<attribute name='new_hopdongvanchuyen' />");
        xml.push("<attribute name='new_hopdongthuhoach' />");
        xml.push("<attribute name='new_hopdongdautumia' />");
        xml.push("<attribute name='new_hdtthatang' />");
        xml.push("<attribute name='new_hopdongdaututrangthietbi' />");
        xml.push("<attribute name='new_loaihopdong' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_phieutamungid' operator='eq' uitype='new_phieutamung' value='" + pdntu[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0) {
                if (rs[0].attributes.new_hopdongdautumia != null)
                    hddtm = rs[0].attributes.new_hopdongdautumia.guid;
                if (rs[0].attributes.new_hopdongdaututhuedat != null)
                    hddttd = rs[0].attributes.new_hopdongdaututhuedat.guid;
                if (rs[0].attributes.new_hopdongdaututrangthietbi != null)
                    hddtmmtb = rs[0].attributes.new_hopdongdaututrangthietbi.guid;
                if (rs[0].attributes.new_hopdongvanchuyen != null)
                    hdvc = rs[0].attributes.new_hopdongvanchuyen.guid;
                if (rs[0].attributes.new_hopdongthuhoach != null)
                    hdth = rs[0].attributes.new_hopdongthuhoach.guid;
                if (rs[0].attributes.new_loaihopdong != null)
                    loaihd = rs[0].attributes.new_loaihopdong.value;
                if (rs[0].attributes.new_hdtthatang != null)
                    hatang = rs[0].attributes.new_hdtthatang.value;
            }
        },
         function (er) {
             console.log(er.message)
         });

        if (hddtm != null && loaihd == 100000000) {
            var xml = [];
            xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
            xml.push("<entity name='new_hopdongdautumia'>");
            xml.push("<attribute name='new_hopdongdautumiaid' />");
            xml.push("<attribute name='new_masohopdong' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongdautumiaid' operator='eq'  uitype='new_hopdongdautumia' value='" + hddtm + "'/>");
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
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongdautumia' operator='eq' uitype='new_hopdongdautumia' value='" + hddtm + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-L" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        if (hddttd != null && loaihd == 100000001) {
            var xml = [];
            xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
            xml.push("<entity name='new_hopdongthuedat'>");
            xml.push("<attribute name='new_hopdongthuedatid' />");
            xml.push("<attribute name='new_mahopdong' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongthuedatid' operator='eq'  uitype='new_hopdongthuedat' value='" + hddttd + "'/>");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_mahopdong != null) {
                    maHDDTM = rs[0].attributes.new_mahopdong.value;
                    if (maHDDTM != null)
                        tieude += "-" + maHDDTM;
                }
            },
             function (er) {
                 console.log(er.message)
             });
            var xml1 = [];
            xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongdaututhuedat' operator='eq' uitype='new_hopdongdaututhuedat' value='" + hddttd + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-L" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        if (hatang != null && loaihd == 100000002) {
            var xml = [];
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml.push("<entity name='new_hopdongdautuhatang'>");
            xml.push("<attribute name='new_mahopdong' />");
            xml.push("<attribute name='new_hopdongdautuhatangid' />");
            xml.push("<order attribute='new_mahopdong' descending='false' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongdautuhatangid' value='" + hatang + "' />");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_mahopdong != null) {
                    maHDDTM = rs[0].attributes.new_mahopdong.value;
                    if (maHDDTM != null)
                        tieude += "-" + maHDDTM;
                }
            },
             function (er) {
                 console.log(er.message)
             });
            var xml1 = [];
            xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hdtthatang' operator='eq' value='" + hatang + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-L" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        if (hdvc != null && loaihd == 100000004) {
            var xml = [];
            xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
            xml.push("<entity name='new_hopdongvanchuyen'>");
            xml.push("<attribute name='new_hopdongvanchuyenid' />");
            xml.push("<attribute name='new_sohopdongvanchuyen' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongvanchuyenid' operator='eq'  uitype='new_hopdongvanchuyen' value='" + hdvc + "'/>");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_sohopdongvanchuyen != null) {
                    maHDDTM = rs[0].attributes.new_sohopdongvanchuyen.value;
                    if (maHDDTM != null)
                        tieude += "-" + maHDDTM;
                }
            },
             function (er) {
                 console.log(er.message)
             });
            var xml1 = [];
            xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongvanchuyen' operator='eq' uitype='new_hopdongvanchuyen' value='" + hdvc + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-L" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        if (hdth != null && loaihd == 100000005) {
            var xml = [];
            xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
            xml.push("<entity name='new_hopdongthuhoach'>");
            xml.push("<attribute name='new_hopdongthuhoachid' />");
            xml.push("<attribute name='new_sohopdong' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongthuhoachid' operator='eq'  uitype='new_hopdongthuhoach' value='" + hdth + "'/>");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_sohopdong != null) {
                    maHDDTM = rs[0].attributes.new_sohopdong.value;
                    if (maHDDTM != null)
                        tieude += "-" + maHDDTM;
                }
            },
             function (er) {
                 console.log(er.message)
             });
            var xml1 = [];
            xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongthuhoach' operator='eq' uitype='new_hopdongthuhoach' value='" + hdth + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-L" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }
        if (hddtmmtb != null && loaihd == 100000003) {
            var xml = [];
            xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
            xml.push("<entity name='new_hopdongdaututrangthietbi'>");
            xml.push("<attribute name='new_hopdongdaututrangthietbiid' />");
            xml.push("<attribute name='new_sohopdong' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='new_hopdongdaututrangthietbiid' operator='eq'  uitype='new_hopdongdaututrangthietbi' value='" + hddtmmtb + "'/>");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_sohopdong != null) {
                    maHDDTM = rs[0].attributes.new_sohopdong.value;
                    if (maHDDTM != null)
                        tieude += "-" + maHDDTM;
                }
            },
             function (er) {
                 console.log(er.message)
             });
            var xml1 = [];
            xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
            xml1.push("<entity name='new_phieutamung'>");
            xml1.push("<attribute name='new_phieutamungid' alias='sumCT' aggregate='count' />");
            xml1.push("<filter type='and'>");
            xml1.push("<condition attribute='new_hopdongdaututrangthietbi' operator='eq' uitype='new_hopdongdaututrangthietbi' value='" + hddtmmtb + "'/>");
            xml1.push("</filter>");
            xml1.push("</entity>");
            xml1.push("</fetch>");
            CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                    tieude += "-L" + (rs[0].attributes.sumCT.value + 1);
                }
            },
         function (er) {
             console.log(er.message)
         });
        }

        var xml1 = [];
        xml1.push("<fetch mapping='logical' distinct='false' aggregate='true'>");
        xml1.push("<entity name='new_chitietphieudenghitamung'>");
        xml1.push("<attribute name='new_chitietphieudenghitamungid' alias='sumCT' aggregate='count' />");
        xml1.push("<filter type='and'>");
        xml1.push("<condition attribute='new_phieudenghitamung' operator='eq' uitype='new_phieutamung' value='" + pdntu[0].id + "'/>");
        xml1.push("</filter>");
        xml1.push("</entity>");
        xml1.push("</fetch>");
        CrmFetchKit.Fetch(xml1.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs[0].attributes.sumCT != null) {
                tieude += "-CT" + (rs[0].attributes.sumCT.value + 1);
            }
        },
     function (er) {
         console.log(er.message)
     });

    }

    Xrm.Page.getAttribute("new_name").setValue(tieude);
}
function PhieuTamUng(isOnChange) {
    debugger;
    var hddautumia = Xrm.Page.getControl("new_hopdongdautumia");
    var hddaututhuedat = Xrm.Page.getControl("new_hopdongdaututhuedat");
    var hddaututrangthietbi = Xrm.Page.getControl("new_hopdongdaututrangthietbi");
    var hdthuhoach = Xrm.Page.getControl("new_hopdongthuhoach");
    var hdvanchuyen = Xrm.Page.getControl("new_hopdongvanchuyen");
    var hdhatang = Xrm.Page.getControl("new_hddthatang");
    var cthddtm = Xrm.Page.getControl("new_chitiethddtmia");
    var cthddttd = Xrm.Page.getControl("new_chitiethddtthuedat");
    var cthddtttb = Xrm.Page.getControl("new_chitiethddttrangthietbi");
    var ptu = Xrm.Page.getAttribute("new_phieudenghitamung").getValue();
    if (ptu != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml.push("<entity name='new_phieutamung'>");
        xml.push("<attribute name='new_phieutamungid' />");
        xml.push("<attribute name='new_loaihopdong' />");
        xml.push("<attribute name='new_hopdongvanchuyen' />");
        xml.push("<attribute name='new_hopdongthuhoach' />");
        xml.push("<attribute name='new_hopdongdaututrangthietbi' />");
        xml.push("<attribute name='new_hopdongdaututhuedat' />");
        xml.push("<attribute name='new_hdtthatang' />");
        xml.push("<attribute name='new_hopdongdautumia' />");
        xml.push("<order attribute='new_loaihopdong' descending='false' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_phieutamungid' operator='eq' value='" + ptu[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0) {
                if (rs[0].attributes.new_loaihopdong != null) {
                    loaihd = rs[0].attributes.new_loaihopdong.value;
                    if (loaihd != null) {
                        Xrm.Page.getAttribute("new_loaihopdong").setValue(loaihd);
                        Xrm.Page.getAttribute("new_loaihopdong").setSubmitMode("always");
                    }
                    // hddt mía
                    var hdmia = rs[0].attributes.new_hopdongdautumia;
                    hddautumia.setVisible(loaihd == 100000000);
                    cthddtm.setVisible(loaihd == 100000000);
                    if (hdmia != null && loaihd == 100000000) {
                        hddautumia.getAttribute().setValue([{
                            name: hdmia.name,
                            id: hdmia.guid,
                            typename: hdmia.logicalName
                        }]);
                        hddautumia.getAttribute().setSubmitMode("always");
                        filter_cthdmia_theohdmia();
                        load_dientich_trongcthdmia();
                    }
                    // thue dat
                    hddaututhuedat.setVisible(loaihd == 100000001);
                    cthddttd.setVisible(loaihd == 100000001);
                    var hdthuedat = rs[0].attributes.new_hopdongdaututhuedat;
                    if (hdthuedat != null && loaihd == 100000001) {
                        hddaututhuedat.getAttribute().setValue([{
                            name: hddttd.name,
                            id: hddttd.guid,
                            typename: hddttd.logicalName
                        }]);
                        hddaututhuedat.getAttribute().setSubmitMode("always");
                        filter_cthtd_theohddat();
                    }
                    // ha tang
                    hdhatang.setVisible(loaihd == 100000002);
                    var hdht = rs[0].attributes.new_hddthatang;
                    if (hdht != null && loaihd == 100000002) {
                        hddaututhuedat.getAttribute().setValue([{
                            name: hdht.name,
                            id: hdht.guid,
                            typename: hdht.logicalName
                        }]);
                        hddaututhuedat.getAttribute().setSubmitMode("always");
                        filter_cthtd_theohddat();
                    }
                    // may moc thiet bi
                    hddaututrangthietbi.setVisible(loaihd == 100000003);
                    cthddtttb.setVisible(loaihd == 100000001);
                    var hdtthietbi = rs[0].attributes.new_hopdongdaututrangthietbi;
                    if (hdtthietbi != null && loaihd == 100000003) {
                        hdthuhoach.getAttribute().setValue([{
                            name: hdtthietbi.name,
                            id: hdtthietbi.guid,
                            typename: hdtthietbi.logicalName
                        }]);
                        hdthuhoach.getAttribute().setSubmitMode("always");
                    }
                    //hd van chuyen
                    hdvanchuyen.setVisible(loaihd == 100000004);
                    var hdvc = rs[0].attributes.new_hopdongvanchuyen;
                    if (hdvc != null && loaihd == 100000004) {
                        hdvanchuyen.getAttribute().setValue([{
                            name: hdvc.name,
                            id: hdvc.guid,
                            typename: hdvc.logicalName
                        }]);
                        hdvanchuyen.getAttribute().setSubmitMode("always");
                    }
                    //hd thu hoach
                    hdthuhoach.setVisible(loaihd == 100000005);
                    var hdth = rs[0].attributes.new_hopdongthuhoach;
                    if (hdth != null && loaihd == 100000005) {
                        hdthuhoach.getAttribute().setValue([{
                            name: hdth.name,
                            id: hdth.guid,
                            typename: hdth.logicalName
                        }]);
                        hdthuhoach.getAttribute().setSubmitMode("always");
                    }
                }


            }
        },
               function (er) {
                   console.log(er.message)
               });
    }



}
function load_dientich_trongcthdmia() {
    debugger;
    var cthd = Xrm.Page.getAttribute("new_chitiethddtmia").getValue();
    if (cthd != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
        xml.push("<entity name='new_thuadatcanhtac'>");
        xml.push("<attribute name='new_thuadatcanhtacid' />");
        xml.push("<attribute name='new_dientichhopdong' />");
        xml.push("<attribute name='new_dientichconlai' />");
        xml.push("<order attribute='new_dientichconlai' descending='false' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_thuadatcanhtacid' operator='eq' value='" + cthd[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs[0].attributes.new_dientichhopdong != null) {
                Xrm.Page.getAttribute("new_dientich").setValue(rs[0].attributes.new_dientichhopdong.value);
                Xrm.Page.getAttribute("new_dientich").setSubmitMode("always");
            }

        },
         function (er) {
             console.log(er.message)
         });
    }
}
function load_dientich_trongtrangthietbi() {
    var cthd = Xrm.Page.getAttribute("new_chitiethddttrangthietbi").getValue();
    if (cthd != null) {
        var xml = [];
        xml.push("<fetch mapping='logical' count='1' version='1.0' distinct='true' >");
        xml.push("<entity name='new_hopdongdaututrangthietbichitiet'>");
        xml.push("<attribute name='new_hopdongdaututrangthietbichitietid' />");
        xml.push("<attribute name='new_dientich' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongdaututrangthietbichitietid' operator='eq'  value='" + cthd[0].id + "'/>");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs[0].attributes.new_dientich != null) {
                Xrm.Page.getAttribute("new_dientich").setValue(rs[0].attributes.new_dientich.value);
            }

        },
         function (er) {
             console.log(er.message)
         });
    }
}
function kiemtra_dientich() {
    var dientich = Xrm.Page.getAttribute("new_dientich").getValue();
    var dientichung = Xrm.Page.getAttribute("new_dientichtamung").getValue();
    if (dientichung > dientich) {
        Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lòng nhập Diện tích tạm ứng không lớn hơn diện tích. </strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"2\")'> Ẩn thông báo</a>", "WARNING", "2");
        Xrm.Page.getAttribute("new_dientichtamung").setValue(null);
    } else
        Xrm.Page.ui.clearFormNotification("2");
}
function filter_cthdmia_theohdmia() {
    var hd = Xrm.Page.getAttribute("new_hopdongdautumia").getValue();
    var cthd = Xrm.Page.getAttribute("new_chitiethddtmia");
    if (hd != null) {
        var xml = [];
        var viewId = "{5597E67C-FD64-4E3E-8AFE-EBE3E5D78B1C}";
        var entityName = "new_thuadatcanhtac";
        var viewDisplayName = "Chi tiết HĐ đầu tư mía Lookup View";

        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml.push("<entity name='new_thuadatcanhtac'>");
        xml.push("<attribute name='new_thuadatcanhtacid'/>");
        xml.push("<attribute name='new_name' />");
        xml.push("<attribute name='new_thuadat' />");
        xml.push("<attribute name='createdon' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
        xml.push("<condition attribute='new_hopdongdautumia' operator='eq'  value='" + hd[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_thuadatcanhtacid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                    "<row name='result'  " + "id='new_thuadatcanhtacid'>  " +
                                    "<cell name='new_name'   " + "width='200' />  " +
                                    "<cell name='new_thuadat'   " + "width='200' />  " +
                                    "<cell name='createdon'    " + "width='200' />  " +
                                    "</row>   " +
                                 "</grid>   ";
        Xrm.Page.getControl("new_chitiethddtmia").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);

    }

}
function filter_cthtd_theohddat() {
    var hd = Xrm.Page.getAttribute("new_hopdongdaututhuedat").getValue();
    var cthd = Xrm.Page.getAttribute("new_chitiethddtthuedat");
    if (hd != null) {
        var xml = [];
        var viewId = "{50E7A9FE-AF6A-4446-892A-9FC2E1D9A648}";
        var entityName = "new_thuadatcanhtac";
        var viewDisplayName = "Chi tiết HĐ thuê đất Lookup View";

        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml.push("<entity name='new_datthue'>");
        xml.push("<attribute name='new_datthueid'/>");
        xml.push("<attribute name='new_name' />");
        xml.push("<attribute name='createdon' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongthuedat' operator='eq'  value='" + hd[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_datthueid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                    "<row name='result'  " + "id='new_datthueid'>  " +
                                    "<cell name='new_name'   " + "width='200' />  " +
                                    "<cell name='createdon'    " + "width='200' />  " +
                                    "</row>   " +
                                 "</grid>   ";
        Xrm.Page.getControl("new_chitiethddtthuedat").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);

    }

}
function filter_cthttbi_theohddat() {
    var hd = Xrm.Page.getAttribute("new_hopdongdaututrangthietbi").getValue();
    var cthd = Xrm.Page.getAttribute("new_chitiethddttrangthietbi");
    if (hd != null) {
        var xml = [];
        var viewId = "{848E43F4-37F8-4CF9-8184-47281078589F}";
        var entityName = "new_hopdongdaututrangthietbichitiet";
        var viewDisplayName = "Chi tiết HĐ trang thiết bị Lookup View";

        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='true'>");
        xml.push("<entity name='new_hopdongdaututrangthietbichitiet'>");
        xml.push("<attribute name='new_hopdongdaututrangthietbichitietid'/>");
        xml.push("<attribute name='new_name' />");
        xml.push("<attribute name='createdon' />");
        xml.push("<filter type='and'>");
        xml.push("<condition attribute='new_hopdongdaututrangthietbi' operator='eq'  value='" + hd[0].id + "' />");
        xml.push("</filter>");
        xml.push("</entity>");
        xml.push("</fetch>");
        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_hopdongdaututrangthietbichitietid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                    "<row name='result'  " + "id='new_hopdongdaututrangthietbichitietid'>  " +
                                    "<cell name='new_name'   " + "width='200' />  " +
                                    "<cell name='createdon'    " + "width='200' />  " +
                                    "</row>   " +
                                 "</grid>   ";
        Xrm.Page.getControl("new_chitiethddttrangthietbi").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);

    }

}
function load_dinhmucdenghiung() {
    debugger;
    var dientichung = Xrm.Page.getAttribute("new_dientichtamung").getValue();
    var dinhmuc = Xrm.Page.getAttribute("new_dongiadenghiung").getValue();
    if (dientichung != null && dinhmuc != null) {
        Xrm.Page.getAttribute("new_dinhmuctamung").setValue(dientichung * dinhmuc);
        Xrm.Page.getAttribute("new_dinhmuctamung").setSubmitMode("always");
    }

}
function kiemtradongia() {
    var dongia = Xrm.Page.getAttribute("new_dongiadenghiung").getValue();
    var dinhmuc = Xrm.Page.getAttribute("new_dongiadinhmucung").getValue();
    if (dongia > dinhmuc) {
        Xrm.Page.ui.setFormHtmlNotification("<strong>Vui lòng nhập Đơn giá đề nghị ứng không lớn hơn định mức ứng. </strong> <a class='ms-crm-List-Message-Link-Lite' href='javascript:' onclick='Xrm.Page.ui.clearFormNotification(\"1\")'> Ẩn thông báo</a>", "WARNING", "1");
        Xrm.Page.getAttribute("new_dongiadenghiung").setValue(null);
    } else
        Xrm.Page.ui.clearFormNotification("1");

}
function dientichconlai() {
    var dientich = Xrm.Page.getAttribute("new_dientich").getValue();
    var dientichtamung = Xrm.Page.getAttribute("new_dientichtamung").getValue();

    if (dientich != null && dientichtamung != null) {
        var dientichconlai = Xrm.Page.getAttribute("new_dientichconlai");
        dientichconlai.setValue(dientich - dientichtamung);
        dientichconlai.setSubmitMode("always");
    }

}