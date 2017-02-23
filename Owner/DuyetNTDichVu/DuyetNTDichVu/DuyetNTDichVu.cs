using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.Script.Serialization;

namespace DuyetNTDichVu
{
    public class DuyetNTDichVu : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            //throw new Exception(target.Id.ToString());

            if (context.Depth > 1)
            {
                return;
            }

            if (context.MessageName == "Create" || context.MessageName == "Update")
            {
                traceService.Trace("chay if thu 1");
                if (target.Contains("new_tinhtrangduyet") && ((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000006) // da duyet
                {
                    decimal tylethanhtoan = 0;

                    Entity ntdichvu = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_phieudangkydichvu", "new_tylethanhtoan", "new_hopdongdautumia", "statuscode" }));
                    traceService.Trace("1");

                    if (!ntdichvu.Contains("new_phieudangkydichvu") || ntdichvu["new_phieudangkydichvu"] == null)
                        return;

                    Entity pdkDichvu = service.Retrieve("new_phieudangkydichvu", ((EntityReference)ntdichvu["new_phieudangkydichvu"]).Id,
                        new ColumnSet(new string[] { "new_danghiemthu", "new_name", "statuscode" }));


                    List<Entity> lstChitietNTDichVu = RetrieveMultiRecord(service, "new_chitietnghiemthudichvu",
                        new ColumnSet(new string[] { "new_thuadat", "new_tienthanhtoan", "new_tieuchuancongviec" }), "new_nghiemthudichvu", ntdichvu.Id);

                    Entity vudautuhientai = getVDThientai();

                    if (!ntdichvu.Contains("new_hopdongdautumia"))
                        throw new Exception("NT dịch vụ không có hợp đồng đầu tư mía ");

                    Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)ntdichvu["new_hopdongdautumia"]).Id,
                        new ColumnSet(new string[] { "new_hopdongdautumiaid" }));

                    foreach (Entity en in lstChitietNTDichVu)
                    {
                        if (!en.Contains("new_thuadat"))
                            continue;

                        QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                        q.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_name", "new_dachihoanlai_dichvu" });
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                        q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongdautumia.Id));

                        Entity tdctUpdate = service.RetrieveMultiple(q).Entities.FirstOrDefault();

                        tdctUpdate["new_dachihoanlai_dichvu"] = en.Contains("new_tienthanhtoan") ? en["new_tienthanhtoan"] : new Money(0);

                        //service.Update(tdctUpdate);
                        tylethanhtoan += en.Contains("new_tieuchuancongviec") ? (decimal)en["new_tieuchuancongviec"] : 0;
                    }

                    decimal danghiemthu = pdkDichvu.Contains("new_danghiemthu") ? (decimal)pdkDichvu["new_danghiemthu"] : 0;

                    if (danghiemthu >= 100)
                        throw new Exception(pdkDichvu["new_name"].ToString() + " đã nghiệm thu xong");

                    danghiemthu += tylethanhtoan;

                    if (danghiemthu >= 100)
                        danghiemthu = 100;

                    //throw new Exception(danghiemthu.ToString());
                    pdkDichvu["new_danghiemthu"] = danghiemthu;

                    if (danghiemthu >= 100)
                        pdkDichvu["statuscode"] = new OptionSetValue(100000001); // huy

                    service.Update(pdkDichvu);
                }
                else
                {
                    traceService.Trace("chay else");
                    Entity ntdichvu = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_phieudangkydichvu",
                            "new_hopdongcungungdichvu", "subject","new_doitactcungcapdichvu","new_doitactcungcapdichvukhdn" }));
                    
                    if (!ntdichvu.Contains("new_phieudangkydichvu") || ntdichvu["new_phieudangkydichvu"] == null)
                        return;

                    Entity pdkDichvu = service.Retrieve("new_phieudangkydichvu", ((EntityReference)ntdichvu["new_phieudangkydichvu"]).Id,
                        new ColumnSet(new string[] { "new_danghiemthu", "new_name",
                            "new_hopdongcungcapdichvu","new_doitaccungcapdichvukh","new_doitaccungcapdichvukhdn" }));

                    if (!pdkDichvu.Contains("new_hopdongcungcapdichvu"))
                        throw new Exception("Phiếu đăng ký dịch vụ không có hợp đồng cung cấp dịch vụ");

                    ntdichvu["new_hopdongcungungdichvu"] = pdkDichvu["new_hopdongcungcapdichvu"];

                    if (pdkDichvu.Contains("new_doitaccungcapdichvukh"))
                        ntdichvu["new_doitactcungcapdichvu"] = pdkDichvu["new_doitaccungcapdichvukh"];

                    if (pdkDichvu.Contains("new_doitaccungcapdichvukhdn"))
                        ntdichvu["new_doitactcungcapdichvukhdn"] = pdkDichvu["new_doitaccungcapdichvukhdn"];

                    service.Update(ntdichvu);                    

                    Entity hdcudv = service.Retrieve("new_hopdongcungungdichvu",
                        ((EntityReference)pdkDichvu["new_hopdongcungcapdichvu"]).Id, new ColumnSet(new string[] { "new_sohopdong" }));

                    if (pdkDichvu.Contains("new_danghiemthu") && (decimal)pdkDichvu["new_danghiemthu"] >= 100)
                        throw new Exception(pdkDichvu["new_name"].ToString() + " đã nghiệm thu xong");

                    QueryExpression q = new QueryExpression("new_chitietdangkydichvu");
                    q.ColumnSet = new ColumnSet(true);
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("new_phieudangkydichvu", ConditionOperator.Equal, pdkDichvu.Id));
                    
                    EntityCollection entc = service.RetrieveMultiple(q);
                    traceService.Trace("retrieve chi tiet dk dich vu");
                    
                    List<Entity> lstChitietDKDVCu = RetrieveMultiRecord(service, "new_chitietnghiemthudichvu",
                        new ColumnSet(new string[] { "new_chitietnghiemthudichvuid" }), "new_nghiemthudichvu", ntdichvu.Id);

                    foreach (Entity en in lstChitietDKDVCu)
                    {
                        service.Delete(en.LogicalName, en.Id);
                    }

                    int i = 1;
                    string[] ntdvName = ntdichvu["subject"].ToString().Split('-');

                    foreach (Entity t in entc.Entities)
                    {
                        Entity temp = new Entity("new_chitietnghiemthudichvu");
                        StringBuilder str = new StringBuilder();
                        str.Append("CTNTDV-" + hdcudv["new_sohopdong"].ToString());

                        if (ntdvName.Length >= 4)
                        {
                            str.Append(ntdvName[3] + "-CT" + i);
                        }

                        temp["new_name"] = str.ToString();

                        if (t.Contains("new_dichvu"))
                            temp["new_dichvu"] = t["new_dichvu"];

                        temp["new_nghiemthudichvu"] = ntdichvu.ToEntityReference();

                        if (t.Contains("new_uom")) // don vi tinh
                            temp["new_uom"] = t["new_uom"];

                        if (t.Contains("new_dongia")) // don gia
                            temp["new_dongia"] = t["new_dongia"];

                        if (t.Contains("new_soluong")) // so luong
                            temp["new_khoiluongthuchien"] = t["new_soluong"];

                        if (t.Contains("new_thanhtien")) // thanh tien
                            temp["new_thanhtien"] = t["new_thanhtien"];

                        if (t.Contains("new_thuadat")) // thua dat
                            temp["new_thuadat"] = t["new_thuadat"];

                        int solan = 0;

                        if (t.Contains("new_solan"))
                        {
                            solan = (int)t["new_solan"];
                            temp["new_solanthuchien"] = solan;
                            //throw new Exception(solan.ToString());
                        }

                        if (t.Contains("new_sotienhl"))
                            temp["new_sotienhl"] = t["new_sotienhl"];

                        if (t.Contains("new_sotienkhl"))
                            temp["new_sotienkhl"] = t["new_sotienkhl"];

                        service.Create(temp);
                        i++;
                        JavaScriptSerializer js = new JavaScriptSerializer();
                        //throw new Exception(js.Serialize(temp));
                        //throw new Exception(((EntityReference)temp["new_nghiemthudichvu"]).LogicalName + "-" + ((Money)temp["new_thanhtien"]).Value.ToString()
                        //    + ntdichvu["subject"].ToString());
                    }
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }

        Entity getVDThientai()
        {
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_danghoatdong", ConditionOperator.Equal, true));

            EntityCollection entc = service.RetrieveMultiple(q);
            return entc.Entities.FirstOrDefault();
        }
    }
}
