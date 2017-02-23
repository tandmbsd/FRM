using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace ActionCopy_BangGiaVattuKhac
{
    public class ActionCopy_BangGiaVattuKhac : IPlugin
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

            if (target.LogicalName == "new_banggiavattukhac")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bgvattu = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (bgvattu == null)
                {
                    throw new Exception("Bảng giá vật tư này không tồn tại !!");
                }
                Entity new_bgvatu = new Entity("new_banggiavattukhac");

                string newName = bgvattu.Contains("new_name") ? bgvattu["new_name"].ToString() : "";
                new_bgvatu["new_name"] = "New - " + newName;
                new_bgvatu["new_ngayapdung"] = bgvattu["new_ngayapdung"];
                new_bgvatu["new_vudautu"] = new EntityReference("new_vudautu", vudautu.Id);

                new_bgvatu["statuscode"] = new OptionSetValue(1);
                Guid new_bgvattuID = service.Create(new_bgvatu);

                List<Entity> lstChitietbanggiavattu = RetrieveMultiRecord(service, "new_chitietbanggiavattukhac", new ColumnSet(true), "new_banggiavattukhac", bgvattu.Id);
                if (lstChitietbanggiavattu.Count > 0)
                {
                    foreach (Entity en in lstChitietbanggiavattu)
                    {
                        Entity new_Chitietbgvattu = new Entity("new_chitietbanggiavattukhac");
                        new_Chitietbgvattu["new_name"] = en["new_name"];
                        new_Chitietbgvattu["new_ngayapdung"] = en["new_ngayapdung"];

                        Entity bgvattuEn = new Entity("new_banggiavattukhac");
                        bgvattuEn.Id = new_bgvattuID;
                        new_Chitietbgvattu["new_banggiavattukhac"] = bgvattuEn.ToEntityReference();
                        new_Chitietbgvattu["new_vattukhac"] = en["new_vattukhac"];
                        new_Chitietbgvattu["new_dongia"] = en["new_dongia"];
                        if (en.Contains("new_donvitinh"))
                        {
                            new_Chitietbgvattu["new_donvitinh"] = en["new_donvitinh"];
                        }

                        service.Create(new_Chitietbgvattu);
                    }
                }

                context.OutputParameters["ReturnId"] = new_bgvattuID.ToString();
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
