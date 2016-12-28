using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Action
{
    public class ActionCopy_BangGiaThuoc : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];

            string input = (string)context.InputParameters["VudautuID"];
            Guid vudautuid = new Guid(input);

            Entity vudautu = service.Retrieve("new_vudautu", vudautuid, new ColumnSet(new string[] { "new_vudautuid" }));

            if (target.LogicalName == "new_banggiathuoc")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bgthuoc = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (bgthuoc == null)
                {
                    throw new Exception("Bảng giá thuốc này không tồn tại !!");
                }
                Entity new_bgthuoc = new Entity("new_banggiathuoc");

                string newName = bgthuoc.Contains("new_name") ? bgthuoc["new_name"].ToString() : "";
                new_bgthuoc["new_name"] = "New - " + newName;
                new_bgthuoc["new_ngayapdung"] = bgthuoc["new_ngayapdung"];
                new_bgthuoc["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);
                new_bgthuoc["statuscode"] = new OptionSetValue(1);
                Guid new_bgthuocID = service.Create(new_bgthuoc);

                List<Entity> lstChitietbanggiathuoc = RetrieveMultiRecord(service, "new_chitietbanggiathuoc", new ColumnSet(true), "new_banggiathuoc", bgthuoc.Id);
                if (lstChitietbanggiathuoc.Count > 0)
                {
                    foreach (Entity en in lstChitietbanggiathuoc)
                    {
                        Entity new_Chitietbgthuoc = new Entity("new_chitietbanggiathuoc");
                        new_Chitietbgthuoc["new_name"] = en["new_name"];
                        new_Chitietbgthuoc["new_ngayapdung"] = en["new_ngayapdung"];

                        Entity bgthuocEn = new Entity("new_banggiathuoc");
                        bgthuocEn.Id = new_bgthuocID;
                        new_Chitietbgthuoc["new_banggiathuoc"] = bgthuocEn.ToEntityReference();
                        new_Chitietbgthuoc["new_thuoc"] = en["new_thuoc"];
                        new_Chitietbgthuoc["new_dongia"] = en["new_dongia"];
                        if (en.Contains("new_donvitinh"))
                        {
                            new_Chitietbgthuoc["new_donvitinh"] = en["new_donvitinh"];
                        }

                        service.Create(new_Chitietbgthuoc);
                    }
                }

                context.OutputParameters["ReturnId"] = new_bgthuocID.ToString();
            }
        }

        EntityReference FindVudautu(EntityReference vudautuRef)
        {
            Entity CurrVudautu = service.Retrieve("new_vudautu", vudautuRef.Id, new ColumnSet(true));
            QueryExpression q = new QueryExpression("new_vudautu");
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
            EntityCollection entc = service.RetrieveMultiple(q);

            List<Entity> lst = entc.Entities.OrderBy(p => p.GetAttributeValue<DateTime>("new_ngaybatdau")).ToList<Entity>();
            int curr = lst.FindIndex(p => p.Id == CurrVudautu.Id);
            if (curr == lst.Count - 1)
            {
                throw new Exception("Không tồn tại vụ đầu tư mới hơn !!!");
            }
            return lst[curr + 1].ToEntityReference();

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
    }
}
