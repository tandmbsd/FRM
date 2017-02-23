using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_BangGiaPhanBon
{
    public class ActionCopy_BangGiaPhanBon : IPlugin
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

            if (target.LogicalName == "new_banggiaphanbon")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bgphanbon = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (bgphanbon == null)
                {
                    throw new Exception("Bảng giá phân bón này không tồn tại !!");
                }
                Entity new_bgphanbon = new Entity("new_banggiaphanbon");

                string newName = bgphanbon.Contains("new_name") ? bgphanbon["new_name"].ToString() : "";
                new_bgphanbon["new_name"] = "New - " + newName;
                new_bgphanbon["new_ngayapdung"] = bgphanbon["new_ngayapdung"];
                new_bgphanbon["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);
                new_bgphanbon["statuscode"] = new OptionSetValue(1);
                
                Guid new_bgphanbonID = service.Create(new_bgphanbon);

                List<Entity> lstChitietbanggiaphanbon = RetrieveMultiRecord(service, "new_chitietbanggiaphanbon", new ColumnSet(true), "new_banggiaphanbon", bgphanbon.Id);
                if (lstChitietbanggiaphanbon.Count > 0)
                {
                    foreach (Entity en in lstChitietbanggiaphanbon)
                    {
                        Entity new_Chitietbgphanbon = new Entity("new_chitietbanggiaphanbon");
                        new_Chitietbgphanbon["new_name"] = en["new_name"];
                        new_Chitietbgphanbon["new_ngayapdung"] = en["new_ngayapdung"];

                        Entity bgphanbonEn = new Entity("new_banggiaphanbon");
                        bgphanbonEn.Id = new_bgphanbonID;
                        new_Chitietbgphanbon["new_banggiaphanbon"] = bgphanbonEn.ToEntityReference();
                        new_Chitietbgphanbon["new_phanbon"] = en["new_phanbon"];
                        new_Chitietbgphanbon["new_dongia"] = en["new_dongia"];

                        if (en.Contains("new_donvitinh"))
                        {
                            new_Chitietbgphanbon["new_donvitinh"] = en["new_donvitinh"];
                        }

                        service.Create(new_Chitietbgphanbon);
                    }
                }

                context.OutputParameters["ReturnId"] = new_bgphanbonID.ToString();
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
