function filter_lydo_theodenghiruthoso() {
    debugger;
    var xml = [];
    var viewId = "{F32D6A97-19A2-4649-8E75-F34620B696B2}";
    var entityName = "new_lydo";
    var viewDisplayName = "Lý do Lookup View";

    xml.push("<fetch mapping='logical' version='1.0' distinct='true' >");
    xml.push("<entity name='new_lydo'>");
    xml.push("<attribute name='new_lydoid' />");
    xml.push("<attribute name='new_name' />");
    xml.push("<attribute name='createdon' />");
    xml.push("<order attribute='new_name' descending='false' /> ");
    xml.push("<filter type='and' >");
    xml.push("<condition attribute='new_entitylabel' operator='eq'  value='Đề nghị rút hồ sơ' />");
    xml.push("</filter>");
    xml.push("</entity>");
    xml.push("</fetch>");
    var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_lydoid'  " + "select='1'  " + "icon='1'  " + "preview='1'>  " +
                                        "<row name='result'  " + "id='new_lydoid'>  " +
                                        "<cell name='new_name'   " + "width='200' />  " +
                                        "<cell name='createdon'    " + "width='200' />  " +
                                        "</row>   " +
                                     "</grid>   ";
    Xrm.Page.getControl("new_lydo").addCustomView(viewId, entityName, viewDisplayName, xml.join(""), layoutXml, true);
}
function sum_giatritaisanthechapconlai() {
    var tonggiatri = Xrm.Page.getAttribute("new_tonggiatritaisandangthechap").getValue();
    var giatridenghirut = Xrm.Page.getAttribute("new_giatridenghirut").getValue();
    var giatrithevao = Xrm.Page.getAttribute("new_giatritaisanthevao").getValue();
    Xrm.Page.getAttribute("new_giatritstcconlai").setValue(tonggiatri - giatridenghirut + giatrithevao);
}
function filter_HDTC_Nguoidenghi(IsOnChange) {
    debugger;
    var kh = Xrm.Page.getAttribute("new_multi_nguoidenghi").getValue();
    if (kh != null) {
        var entityName = "new_hopdongthechap";
        var viewDisplayName = "Hợp đồng TSTC thế vào Lookup View";
        var viewId = "{3D963876-3C2E-4215-A985-9749B2ED34E6}";
        var xml1 = [];
        if (kh[0].type == 2)
            var khcn = kh;
        if (kh[0].type == 1)
            var khdn = kh;
        xml1.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'> ");
        xml1.push("<entity name='new_hopdongthechap'> ");
        xml1.push("<attribute name='new_hopdongthechapid' /> ");
        xml1.push("<attribute name='new_name' /> ");
        xml1.push("<attribute name='createdon' /> ");
        xml1.push("<filter>");
        xml1.push("<filter type='or'> ");
        if (khcn != null)
            xml1.push("<condition attribute='new_nguoidambaochinhkhcn' operator='eq' value='" + khcn[0].id + "' /> ");
        if (khdn != null)
            xml1.push("<condition attribute='new_nguoiduocdambaochinhkhdn' operator='eq' value='" + khdn[0].id + "' /> ");
        if (khcn != null)
            xml1.push("<condition attribute='new_chuhopdong' operator='eq' value='" + khcn[0].id + "' /> ");
        if (khdn != null)
            xml1.push("<condition attribute='new_chuhopdongdoanhnghiep' operator='eq' value='" + khdn[0].id + "' /> ");
        xml1.push("</filter>");
        xml1.push("<filter>");
        xml1.push("<condition attribute='statuscode' operator='ne' value='100000001' />");
        xml1.push("</filter>");
        xml1.push("</filter>");
        xml1.push("</entity>");
        xml1.push("</fetch>");

        var layoutXml = "<grid name='resultset' " + "object='1' " + "jump='new_hopdongthechapid'  " + "select='1'  " + "icon='0'  " + "preview='0'>  " +
                            "<row name='result'  " + "id='new_hopdongthechapid'>  " +
                            "<cell name='new_name'   " + "width='200' />  " +
                            "<cell name='createdon'    " + "width='100' />  " +
                            "</row>   " +
                         "</grid>   ";
        Xrm.Page.getControl("new_hopdongtstcthevao").addCustomView(viewId, entityName, viewDisplayName, xml1.join(""), layoutXml, true);


    }

    if (IsOnChange == true) {
        Xrm.Page.getAttribute("new_hopdongtstcthevao").setValue(null);
    }
}
function setvalue_GiaTriTSthevao_HDTC() {
    debugger;
    var hdtc = Xrm.Page.getAttribute("new_hopdongtstcthevao").getValue();
    var tonggiatri = 0;
    if (hdtc != null) {
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false' aggregate='true'>");
        xml.push(" <entity name='new_taisanthechap' >");
        xml.push("<attribute name='new_giatridinhgiagiatrithechap' alias='sum_new_giatridinhgiagiatrithechap' aggregate='sum' />");
        xml.push(" <filter type='and' >");
        xml.push(" <condition attribute='statuscode' operator='eq' value='100000000' />");
        xml.push("</filter>");
        xml.push("<link-entity name='new_hopdongthechap' from='new_hopdongthechapid' to='new_hopdongthechap' alias='ae' >");
        xml.push(" <filter type='and' >");
        xml.push(" <condition attribute='new_hopdongthechapid' operator='eq' value='" + hdtc[0].id + "' />");
        xml.push(" </filter>");
        xml.push(" </link-entity>");
        xml.push(" </entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs[0].attributes.sum_new_giatridinhgiagiatrithechap.value != "") {
                tonggiatri = rs[0].attributes.sum_new_giatridinhgiagiatrithechap.value;
                Xrm.Page.getAttribute("new_giatritaisanthevao").setValue(tonggiatri);
                Xrm.Page.getAttribute("new_giatritaisanthevao").setSubmitMode("always");
            } else
                Xrm.Page.getAttribute("new_giatritaisanthevao").setValue(0);

        },
         function (er) {
             console.log(er.message)
         });
    }


}
function sum_giatritaisanthechapconlai(isOnchange) {
    var tonggiatri = Xrm.Page.getAttribute("new_tonggiatritaisandangthechap").getValue();
    var giatridenghirut = Xrm.Page.getAttribute("new_giatridenghirut").getValue();
    var giatrithevao = Xrm.Page.getAttribute("new_giatritaisanthevao").getValue();
    var conlai = Xrm.Page.getAttribute("new_giatritstcconlai").getValue();
    if (conlai == null || conlai == 0 || isOnchange == true) {
        Xrm.Page.getAttribute("new_giatritstcconlai").setValue(tonggiatri - giatridenghirut + giatrithevao);
        Xrm.Page.getAttribute("new_giatritstcconlai").setSubmitMode("always");
        Xrm.Page.data.save();
    }
}
function sum_Tonggiatritaisan() {
    var tonggiatri = 0;
    var kh = Xrm.Page.getAttribute("new_multi_nguoidenghi").getValue();
    //lấy tổng giá trị thế chấp 2 bên theo chủ hợp đồng
    if (kh != null) {
        if (kh[0].type == 2)
            var khcn = kh;
        if (kh[0].type == 1)
            var khdn = kh;
        var xml = [];

        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false' aggregate='true' >");
        xml.push("<entity name='new_taisanthechap' >");
        xml.push("<attribute name='new_giatridinhgiagiatrithechap' alias='sum_new_giatridinhgiagiatrithechap' aggregate='sum'/>");
        xml.push(" <filter type='and' >");
        xml.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
        xml.push("</filter>");
        xml.push(" <link-entity name='new_hopdongthechap' from='new_hopdongthechapid' to='new_hopdongthechap' alias='ac' >");
        xml.push(" <filter type='and' >");
        xml.push("<condition attribute='new_benthuba' operator='eq'  value='0'/>");
        if (khcn != null)
            xml.push("<condition attribute='new_chuhopdong' operator='eq'  value='" + khcn[0].id + "' />");
        if (khdn != null)
            xml.push("<condition attribute='new_chuhopdongdoanhnghiep' operator='eq'  value='" + khdn[0].id + "'/>");
        xml.push(" </filter>");
        xml.push(" </link-entity>");
        xml.push(" </entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs[0].attributes.sum_new_giatridinhgiagiatrithechap.value != "") {
                tonggiatri = rs[0].attributes.sum_new_giatridinhgiagiatrithechap.value;

            }

        },
         function (er) {
             console.log(er.message)
         });

    }
    //lấy tổng giá trị thế chấp theo bên thứ 3
    if (kh != null) {
        if (kh[0].type == 2)
            var khcn = kh;
        if (kh[0].type == 1)
            var khdn = kh;
        var xml = [];
        xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false' aggregate='true' >");
        xml.push("<entity name='new_taisanthechap' >");
        xml.push("<attribute name='new_giatridinhgiagiatrithechap' alias='sum_new_giatridinhgiagiatrithechap' aggregate='sum'/>");
        xml.push(" <filter type='and' >");
        xml.push("<condition attribute='statuscode' operator='eq' value='100000000' />");
        xml.push("</filter>");
        xml.push(" <link-entity name='new_hopdongthechap' from='new_hopdongthechapid' to='new_hopdongthechap' alias='ac' >");
        xml.push(" <filter type='and' >");
        xml.push("<condition attribute='new_benthuba' operator='eq'  value='1'/>");
        if (khcn != null)
            xml.push("<condition attribute='new_nguoidambaochinhkhcn' operator='eq'  value='" + khcn[0].id + "' />");
        if (khdn != null)
            xml.push("<condition attribute='new_nguoiduocdambaochinhkhdn' operator='eq'  value='" + khdn[0].id + "'/>");
        xml.push(" </filter>");
        xml.push(" </link-entity>");
        xml.push(" </entity>");
        xml.push("</fetch>");
        CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
            if (rs.length > 0 && rs[0].attributes.sum_new_giatridinhgiagiatrithechap.value != "") {
                tonggiatri += rs[0].attributes.sum_new_giatridinhgiagiatrithechap.value;

            }

        },
         function (er) {
             console.log(er.message)
         });

    }
    Xrm.Page.getAttribute("new_tonggiatritaisandangthechap").setValue(tonggiatri);
    Xrm.Page.getAttribute("new_tonggiatritaisandangthechap").setSubmitMode("always");
}
function datten_denghiruthoso() {
    var kh = Xrm.Page.getAttribute("new_multi_nguoidenghi").getValue();
    if (kh != null) {
        if (kh[0].type == 2)
            var khcn = kh;
        else if (kh[0].type == 1)
            var khdn = kh;
        var tieude = "Đề nghị rút hồ sơ-" + kh[0].name;
        if (khcn != null) {
            var xml = [];
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml.push("<entity name='contact'>");
            xml.push("<attribute name='contactid' />");
            xml.push("<attribute name='new_socmnd' />");
            xml.push("<order attribute='new_socmnd' descending='false' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='contactid' operator='eq' value='" + khcn[0].id + "' />");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_socmnd != "") {
                    tieude += "-" + rs[0].attributes.new_socmnd.value;
                }

            },
             function (er) {
                 console.log(er.message)
             });
        } else if (khdn != null) {
            var xml = [];
            xml.push("<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>");
            xml.push("<entity name='account'>");
            xml.push("<attribute name='accountid' />");
            xml.push("<attribute name='new_masothue' />");
            xml.push("<order attribute='new_masothue' descending='false' />");
            xml.push("<filter type='and'>");
            xml.push("<condition attribute='accountid' operator='eq' value='" + khdn[0].id + "' />");
            xml.push("</filter>");
            xml.push("</entity>");
            xml.push("</fetch>");
            CrmFetchKit.Fetch(xml.join(""), false).then(function (rs) {
                if (rs.length > 0 && rs[0].attributes.new_masothue != "") {
                    tieude += "-" + rs[0].attributes.new_masothue.value;
                }

            },
             function (er) {
                 console.log(er.message)
             });
        }
        Xrm.Page.getAttribute("new_name").setValue(tieude);

    }
}
