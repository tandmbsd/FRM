using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace ActionCopy_BangGiaDichVu
{
    public class ActionCopy_BangGiaDichVu : IPlugin
    {
        private IOrganizationServiceFactory factory;
        private IOrganizationService service;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext) serviceProvider.GetService(typeof(IPluginExecutionContext));
            var target = (EntityReference) context.InputParameters["Target"];

            var input = (string) context.InputParameters["VudautuID"];
            var vudautuid = new Guid(input);

            var vudautu = service.Retrieve("new_vudautu", vudautuid, new ColumnSet("new_vudautuid"));

            if (target.LogicalName == "new_banggiadichvu")
            {
                factory = (IOrganizationServiceFactory) serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                var bgdv = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (bgdv == null)
                    throw new Exception("Bảng giá dịch vụ này không tồn tại !!");
                var new_bgdv = new Entity("new_banggiadichvu");

                var newName = bgdv.Contains("new_name") ? bgdv["new_name"].ToString() : "";
                new_bgdv["new_name"] = "New - " + newName;
                new_bgdv["new_thoidiemapdung"] = bgdv["new_thoidiemapdung"];
                new_bgdv["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);
                new_bgdv["statuscode"] = new OptionSetValue(1);
                var new_bgdvID = service.Create(new_bgdv);

                var lstChitietbanggiadichvu = RetrieveMultiRecord(service, "new_chitietbanggiadichvu",
                    new ColumnSet(true), "new_banggiadichvu", bgdv.Id);
                if (lstChitietbanggiadichvu.Count > 0)
                    foreach (var en in lstChitietbanggiadichvu)
                    {
                        var new_Chitietbgdv = new Entity("new_chitietbanggiadichvu");
                        new_Chitietbgdv["new_name"] = en["new_name"];
                        new_Chitietbgdv["new_ngayapdung"] = en["new_ngayapdung"];

                        var bgdvEn = new Entity("new_banggiadichvu");
                        bgdvEn.Id = new_bgdvID;
                        new_Chitietbgdv["new_banggiadichvu"] = bgdvEn.ToEntityReference();

                        if (!en.Contains("new_dichvu"))
                            throw new Exception("Chi tiết bảng giá dịch vụ " + en["new_name"] + " không có dịch vụ");
                        new_Chitietbgdv["new_dichvu"] = en["new_dichvu"];
                        new_Chitietbgdv["new_gia"] = en["new_gia"];

                        if (en.Contains("new_donvitinh"))
                            new_Chitietbgdv["new_donvitinh"] = en["new_donvitinh"];
                        service.Create(new_Chitietbgdv);
                    }

                var lstTanggiamgiadichvu = RetrieveMultiRecord(service, "new_tanggiamgiadichvutheovung",
                    new ColumnSet(true), "new_banggiadichvu", bgdv.Id);
                if (lstTanggiamgiadichvu.Count > 0)
                    foreach (var en in lstTanggiamgiadichvu)
                    {
                        var new_tanggiamgiadichvu = new Entity("new_tanggiamgiadichvutheovung");
                        new_tanggiamgiadichvu["new_name"] = en["new_name"];

                        var bgdvEn = new Entity("new_banggiadichvu");
                        bgdvEn.Id = new_bgdvID;
                        new_tanggiamgiadichvu["new_banggiadichvu"] = bgdvEn.ToEntityReference();

                        new_tanggiamgiadichvu["new_ngayapdung"] = en["new_ngayapdung"];
                        new_tanggiamgiadichvu["new_vungdialy"] = en["new_vungdialy"];
                        new_tanggiamgiadichvu["new_sotien"] = en["new_sotien"];
                        service.Create(new_tanggiamgiadichvu);
                    }
                context.OutputParameters["ReturnId"] = new_bgdvID.ToString();
            }
        }

        private EntityReference FindVudautu(EntityReference vudautuRef)
        {
            var CurrVudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(true));
            var q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            var entc = service.RetrieveMultiple(q);

            var lst = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList();
            var curr = lst.FindIndex(p => p.Id == CurrVudautu.Id);
            if (curr == lst.Count - 1)
                throw new Exception("Không tồn tại vụ đầu tư mới hơn !!!");
            return lst[curr + 1].ToEntityReference();
        }

        private List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column,
            string condition, object value)
        {
            var q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            var entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList();
        }
    }
}