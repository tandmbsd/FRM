using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Action_LayPhieu_BangKeTienMia
{
    public class Action_LayPhieu_BangKeTienMia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            EntityReference target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_bangketienmia")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity bangketienmia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                Guid id = bangketienmia.Id;

                if (bangketienmia == null)
                {
                    throw new Exception("Bảng kê tiền mía này không tòn tại !!! ");
                }

                EntityCollection RefLenhdon = RetrieveNNRecord(service, "new_lenhdon", "new_bangketienmia", "new_new_bangketienmia_new_lenhdon",
                    new ColumnSet(true), "new_bangketienmiaid", bangketienmia.Id);
                EntityReferenceCollection entcReflenhdon = new EntityReferenceCollection();

                foreach (Entity en in RefLenhdon.Entities)
                {
                    entcReflenhdon.Add(en.ToEntityReference());
                }
                
                service.Disassociate("new_bangketienmia", bangketienmia.Id, new Relationship("new_new_bangketienmia_new_lenhdon"), entcReflenhdon);

                DateTime bDate = (DateTime)bangketienmia["new_tungay"];
                DateTime eDate = (DateTime)bangketienmia["new_denngay"];

                EntityCollection lstKhachhang = RetrieveNNRecord(service, "contact", "new_bangketienmia", "new_new_bangketienmia_contact",
                    new ColumnSet(true), "new_bangketienmiaid", bangketienmia.Id);
                StringBuilder fetchXml = new StringBuilder();

                if (lstKhachhang.Entities.Count > 0) // có khách hàng
                {
                    fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                    fetchXml.Append("<entity name='new_lenhdon'>");
                    fetchXml.Append("<attribute name='new_lenhdonid'/>");
                    fetchXml.Append("<filter type='and'>");
                    fetchXml.Append(string.Format("<condition attribute='new_ngaycap' operator='ge' value='{0}'></condition>", bDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_ngaycap' operator='le' value='{0}'></condition>", eDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='statuscode' operator='eq' value='{0}'></condition>", 100000000));                    
                    fetchXml.Append("<filter type='or'>");

                    foreach (Entity kh in lstKhachhang.Entities)
                    {
                        fetchXml.Append(string.Format("<condition attribute='new_khachhang' operator='eq' value='{0}'></condition>", kh.Id));
                    }

                    fetchXml.Append("</filter>");
                    fetchXml.Append("</filter>");
                    fetchXml.Append("</entity>");
                    fetchXml.Append("</fetch>");
                }
                else // ngược lại 
                {
                    fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                    fetchXml.Append("<entity name='new_lenhdon'>");
                    fetchXml.Append("<attribute name='new_lenhdonid'/>");
                    fetchXml.Append("<filter type='and'>");
                    fetchXml.Append(string.Format("<condition attribute='new_ngaycap' operator='ge' value='{0}'></condition>", bDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_ngaycap' operator='le' value='{0}'></condition>", eDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='statuscode' operator='eq' value='{0}'></condition>", 100000000));
                    fetchXml.Append("</filter>");
                    fetchXml.Append("</entity>");
                    fetchXml.Append("</fetch>");
                }
                EntityCollection entcLenhdon = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                EntityReferenceCollection RefentcLenhdon = new EntityReferenceCollection();
                
                foreach (Entity entc in entcLenhdon.Entities)
                {
                    RefentcLenhdon.Add(entc.ToEntityReference());
                }
                service.Associate("new_bangketienmia", bangketienmia.Id, new Relationship("new_new_bangketienmia_new_lenhdon"), RefentcLenhdon);

                context.OutputParameters["ReturnId"] = id.ToString();
            }
        }

        EntityCollection RetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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

            return collRecords;
        }
    }
}
