// JavaScript source code
function btn_TinhTienKK() {
    try {
        var id = Xrm.Page.data.entity.getId();
        if (id != null) {
            window.top.$ui.Confirm("Xác nhận", "Bạn có muốn tính tiền phiếu khuyến khích ?", function (e) {
                ExecuteAction(
                id
                , "new_bangketienkhuyenkhich"
                , "new_Action_TinhTienKhuyenKhich"
                , null//[{ name: 'ReturnId', type: 'string', value: null }]
                , function (result) {
                    //debugger;
                    //alert("a");
                    if (result != null && result.status != null) {
                        if (result.status == "error")
                            window.top.$ui.Dialog("Lỗi", result.data);
                        else if (result.status == "success") {
                            window.parent.Xrm.Utility.openEntityForm("new_bangketienkhuyenkhich", result.data.ReturnId.value, null, { openInNewWindow: true });
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
