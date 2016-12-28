// JavaScript source code
function btn_CopyBangGia() {
    try {
        var id = Xrm.Page.data.entity.getId();
        if (id != null) {
            window.top.$ui.Confirm("Xác nhận", "Bạn có muốn copy bảng giá vận chuyển này không ?", function (e) {
                ExecuteAction(
                id
                , "new_banggiavanchuyen"
                , "new_ActionCopy_BangGiaVanChuyen"
                , null//[{ name: 'ReturnId', type: 'string', value: null }]
                , function (result) {
                    //debugger;
                    //alert("a");
                    if (result != null && result.status != null) {
                        if (result.status == "error")
                            window.top.$ui.Dialog("Lỗi", result.data);
                        else if (result.status == "success") {
                            window.parent.Xrm.Utility.openEntityForm("new_banggiavanchuyen", result.data.ReturnId.value, null, { openInNewWindow: true });
                        }
                        else {
                            console.log(JSON.stringify(result));
                        }
                    }
                });
            }, null);
        }
        else {
            window.top.$ui.Dialog("Lỗi", "Thông báo lỗi!", null);
        }
    }
    catch (e) {
        window.top.$ui.Dialog("Lỗi", e.message, null);
    }
}
