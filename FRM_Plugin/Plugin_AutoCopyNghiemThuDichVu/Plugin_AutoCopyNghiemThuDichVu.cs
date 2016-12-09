using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoCopyNghiemThuDichVu
{
    public class Plugin_AutoCopyNghiemThuDichVu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            
            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_phieudangkydichvu"))
            {
                Entity Nghiemthu = service.Retrieve("new_nghiemthudichvu", target.Id, new ColumnSet(true));
                List<Entity> DSchitietcu = RetrieveMultiRecord(service, "new_chitietnghiemthudichvu", new ColumnSet(true), "new_nghiemthudichvu", target.Id);
                foreach (Entity a in DSchitietcu)
                    service.Delete(a.LogicalName, a.Id);

                List<Entity> DSChitietDVDangky = RetrieveMultiRecord(service, "new_chitietdangkydichvu", new ColumnSet(true), "new_phieudangkydichvu", ((EntityReference)Nghiemthu["new_phieudangkydichvu"]).Id);
                foreach (Entity a in DSChitietDVDangky)
                {
                    Entity rs = new Entity("new_chitietnghiemthudichvu");
                    rs["new_name"] = "Nghiệm thu " + (a.Attributes.Contains("new_dichvu") ? ((EntityReference)a["new_dichvu"]).Name : " dịch vụ");
                    rs["new_nghiemthudichvu"] = new EntityReference("new_nghiemthudichvu", target.Id);
                    rs["new_dichvu"] = a.Attributes.Contains("new_dichvu") ? a["new_dichvu"] : null;
                    rs["new_khoiluongthuchien"] = a.Attributes.Contains("new_soluong") ? a["new_soluong"] : null;
                    rs["new_uom"] = a.Attributes.Contains("new_uom") ? a["new_uom"] : null;
                    service.Create(rs);
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

        EntityReferenceCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
        {
            EntityReferenceCollection result = new EntityReferenceCollection();
            QueryExpression query = new QueryExpression(entity1);
            query.ColumnSet = column;
            LinkEntity linkEntity1 = new LinkEntity(entity1, relateName, entity1 + "id", entity1 + "id", JoinOperator.Inner);
            LinkEntity linkEntity2 = new LinkEntity(relateName, entity2, entity2 + "id", entity2 + "id", JoinOperator.Inner);

            linkEntity1.LinkEntities.Add(linkEntity2);
            query.LinkEntities.Add(linkEntity1);

            linkEntity2.LinkCriteria = new FilterExpression();
            linkEntity2.LinkCriteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection collRecords = service.RetrieveMultiple(query);

            foreach (Entity a in collRecords.Entities)
            {
                result.Add(new EntityReference(entity1, a.Id));
            }

            return result;
        }
    }
}
