using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_BangGiaGiong
{
    public class ActionCopy_BangGiaGiong:IPlugin
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
            if (target.LogicalName == "new_banggiagiong")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bggiong = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                
                if (bggiong == null)
                {
                    throw new Exception("Bảng giá giống này không tồn tại !!");
                }
                Entity new_bggiong = new Entity("new_banggiagiong");

                string newName = bggiong.Contains("new_name") ? bggiong["new_name"].ToString() : "";
                new_bggiong["new_name"] = "New - " + newName;
                new_bggiong["new_ngayapdung"] = bggiong["new_ngayapdung"];
                new_bggiong["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);
                new_bggiong["statuscode"] = new OptionSetValue(1);
                new_bggiong["new_dongia"] = bggiong["new_dongia"];
                if (bggiong.Contains("new_donvitinh"))
                {
                    new_bggiong["new_donvitinh"] = bggiong["new_donvitinh"];
                }
                
                Guid new_bggiongID = service.Create(new_bggiong);

                List<Entity> lstChitietbanggiathuoc = RetrieveMultiRecord(service, "new_chitietbanggiagiong", new ColumnSet(true), "new_banggiagiong", bggiong.Id);
                if (lstChitietbanggiathuoc.Count > 0)
                {
                    foreach (Entity en in lstChitietbanggiathuoc)
                    {
                        Entity new_Chitietbggiong = new Entity("new_chitietbanggiagiong");
                        new_Chitietbggiong["new_name"] = en["new_name"];
                        new_Chitietbggiong["new_ngayapdung"] = en["new_ngayapdung"];

                        Entity bggiongEn = new Entity("new_banggiagiong");
                        bggiongEn.Id = new_bggiongID;
                        new_Chitietbggiong["new_banggiagiong"] = bggiongEn.ToEntityReference();
                        new_Chitietbggiong["new_giongmia"] = en["new_giongmia"];
                        new_Chitietbggiong["new_dongia"] = en["new_dongia"];

                        if (en.Contains("new_donvitinh"))
                        {
                            new_Chitietbggiong["new_donvitinh"] = en["new_donvitinh"];
                        }

                        service.Create(new_Chitietbggiong);
                    }
                }

                context.OutputParameters["ReturnId"] = new_bggiongID.ToString();
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
