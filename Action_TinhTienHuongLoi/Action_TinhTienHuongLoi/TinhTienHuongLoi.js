﻿// JavaScript source code
function btn_TinhTienHuongLoi() {
    try {
        var id = Xrm.Page.data.entity.getId();
        if (id != null) {
            window.top.$ui.Confirm("Xác nhận", "Bạn có muốn tính lại số tiền nông dân chịu trong chi tiết HDDT hạ tầng không ?", function (e) {
                ExecuteAction(
                id
                , "new_hopdongdautuhatang"
                , "new_Action_TinhTienHuongLoi"
                , null//[{ name: 'ReturnId', type: 'string', value: null }]
                , function (result) {
                    //debugger;
                    
                    if (result != null && result.status != null) {
                        if (result.status == "error")
                            window.top.$ui.Dialog("Lỗi", result.data);
                        else if (result.status == "success") {
                            window.parent.Xrm.Utility.openEntityForm("new_hopdongdautuhatang", result.data.ReturnId.value, null, { openInNewWindow: true });
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

