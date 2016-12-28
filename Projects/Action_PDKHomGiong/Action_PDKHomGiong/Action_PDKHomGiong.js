// JavaScript source code
function btn_GiaoNhan() {
    try {
        var id = Xrm.Page.data.entity.getId();
        if (id != null) {
            ExecuteAction(
            id
            , "new_phieudangkyhomgiong"
            , "new_Action_PDKHomGiong"
            , null//[{ name: 'ReturnId', type: 'string', value: null }]
            , function (result) {
                //debugger;
                //alert("a");
                if (result != null && result.status != null) {
                    if (result.status == "error")
                        window.top.$ui.Dialog("Lỗi", result.data);
                    else if (result.status == "success") {
                        window.parent.Xrm.Utility.openEntityForm("new_phieudangkyhomgiong", result.data.ReturnId.value, null, { openInNewWindow: true });
                    }
                    else {
                        console.log(JSON.stringify(result));
                    }
                }
            });
        }
        else {
            window.top.$ui.Dialog("Lỗi", "Thông báo lỗi!", null);
        }
    }
    catch (e) {
        window.top.$ui.Dialog("Lỗi", e.message, null);
    }
}
