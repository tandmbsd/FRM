using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;


namespace Action_BangChiTienCuoiVu
{
    public class Action_BangChiTienCuoiVu : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            EntityReference target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_bangkechitiencuoivu")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity bangkecuoivu = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));

                if (bangkecuoivu == null)
                {
                    throw new Exception("Bảng kê này không tồn tại !!");
                }

                int loaibangke = ((OptionSetValue)bangkecuoivu["new_loaibangke"]).Value;

                DateTime bDate = (DateTime)bangkecuoivu["new_tungay"];
                DateTime eDate = (DateTime)bangkecuoivu["new_denngay"];
                #region PDN thưởng
                if (loaibangke >= 100000000 && loaibangke <= 100000004) // pdn thưởng
                {
                    EntityReferenceCollection pdnt = RefRetrieveNNRecord(service, "new_phieudenghithuong", "new_bangkechitiencuoivu", "new_new_bangkechitiencuoivu_new_pdnthuong", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkecuoivu.Id);
                    service.Disassociate("new_bangkechitiencuoivu", bangkecuoivu.Id, new Relationship("new_new_bangkechitiencuoivu_new_pdnthuong"), pdnt);

                    StringBuilder fetchXml = new StringBuilder();
                    fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                    fetchXml.Append("<entity name='new_phieudenghithuong'>");
                    fetchXml.Append("<attribute name='new_phieudenghithuongid'/>");
                    fetchXml.Append("<filter type='and'>");
                    fetchXml.Append(string.Format("<condition attribute='new_ngaylapphieu' operator='ge' value='{0}'></condition>", bDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_ngaylapphieu' operator='le' value='{0}'></condition>", eDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_vudautu' operator='eq' value='{0}'></condition>", ((EntityReference)bangkecuoivu["new_vudautu"]).Id));
                    fetchXml.Append(string.Format("<condition attribute='new_loaithuong' operator='eq' value='{0}'></condition>", loaibangke));
                    fetchXml.Append("</filter>");
                    fetchXml.Append("</entity>");
                    fetchXml.Append("</fetch>");

                    EntityCollection entcPDNT = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));
                    EntityReferenceCollection RefcollRecords = new EntityReferenceCollection();

                    foreach (Entity en in entcPDNT.Entities)
                    {
                        RefcollRecords.Add(en.ToEntityReference());
                    }
                    //throw new Exception(bDate.ToString() + "-" + eDate.ToString() + "-" + ((EntityReference)bangkecuoivu["new_vudautu"]).Id.ToString() + "-" +loaibangke.ToString());
                    if (RefcollRecords.Count > 0)
                    {
                        service.Associate("new_bangkechitiencuoivu", target.Id, new Relationship("new_new_bangkechitiencuoivu_new_pdnthuong"), RefcollRecords);
                    }

                }
                #endregion
                #region BBVP
                else if (loaibangke == 100000005) // bien ban vi pham
                {
                    EntityReferenceCollection bbvp = RefRetrieveNNRecord(service, "new_bienbanvipham", "new_bangkechitiencuoivu", "new_new_bangkechitiencuoivu_new_bbvipham", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkecuoivu.Id);
                    service.Disassociate("new_bangkechitiencuoivu", bangkecuoivu.Id, new Relationship("new_new_bangkechitiencuoivu_new_bbvipham"), bbvp);

                    StringBuilder fetchXml = new StringBuilder();
                    fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                    fetchXml.Append("<entity name='new_bienbanvipham'>");
                    fetchXml.Append("<attribute name='new_bienbanviphamid'/>");
                    fetchXml.Append("<filter type='and'>");
                    fetchXml.Append(string.Format("<condition attribute='new_ngaylapbienban' operator='ge' value='{0}'></condition>", bDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_ngaylapbienban' operator='le' value='{0}'></condition>", eDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_vudautu' operator='eq' value='{0}'></condition>", ((EntityReference)bangkecuoivu["new_vudautu"]).Id));
                    fetchXml.Append("</filter>");
                    fetchXml.Append("</entity>");
                    fetchXml.Append("</fetch>");

                    EntityCollection entcBBVP = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));

                    EntityReferenceCollection RefcollRecords = new EntityReferenceCollection();

                    foreach (Entity en in entcBBVP.Entities)
                    {
                        RefcollRecords.Add(en.ToEntityReference());
                    }

                    if (RefcollRecords.Count > 0)
                    {
                        service.Associate("new_bangkechitiencuoivu", target.Id, new Relationship("new_new_bangkechitiencuoivu_new_bbvipham"), RefcollRecords);
                    }
                }
                #endregion
                #region PTTM
                else if (loaibangke == 100000006 || loaibangke == 100000007)  // tam giu or thuong cho xe vc
                {
                    EntityReferenceCollection pttm = RefRetrieveNNRecord(service, "new_phieutinhtienmia", "new_bangkechitiencuoivu", "new_new_bkchitiencuoivu_new_phieuttmia", new ColumnSet(true), "new_bangkechitiencuoivuid", bangkecuoivu.Id);
                    service.Disassociate("new_bangkechitiencuoivu", bangkecuoivu.Id, new Relationship("new_new_bkchitiencuoivu_new_phieuttmia"), pttm);

                    StringBuilder fetchXml = new StringBuilder();
                    fetchXml.Append("<fetch mapping='logical' version='1.0'>");
                    fetchXml.Append("<entity name='new_phieutinhtienmia'>");
                    fetchXml.Append("<attribute name='new_phieutinhtienmiaid'/>");
                    fetchXml.Append("<filter type='and'>");
                    fetchXml.Append(string.Format("<condition attribute='new_ngaylap' operator='ge' value='{0}'></condition>", bDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_ngaylap' operator='le' value='{0}'></condition>", eDate.ToString("O")));
                    fetchXml.Append(string.Format("<condition attribute='new_vudautu' operator='eq' value='{0}'></condition>", ((EntityReference)bangkecuoivu["new_vudautu"]).Id));
                    fetchXml.Append("</filter>");
                    fetchXml.Append("</entity>");
                    fetchXml.Append("</fetch>");

                    EntityCollection entcBBVP = service.RetrieveMultiple(new FetchExpression(fetchXml.ToString()));

                    EntityReferenceCollection RefcollRecords = new EntityReferenceCollection();

                    foreach (Entity en in entcBBVP.Entities)
                    {
                        RefcollRecords.Add(en.ToEntityReference());
                    }

                    if (RefcollRecords.Count > 0)
                    {
                        service.Associate("new_bangkechitiencuoivu", target.Id, new Relationship("new_new_bkchitiencuoivu_new_phieuttmia"), RefcollRecords);
                    }
                }
                #endregion
                context.OutputParameters["ReturnId"] = bangkecuoivu.Id.ToString();
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
        EntityReferenceCollection RefRetrieveNNRecord(IOrganizationService crmservices, string entity1, string entity2, string relateName, ColumnSet column, string condition, object value)
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
            EntityReferenceCollection RefcollRecords = new EntityReferenceCollection();

            foreach (Entity en in collRecords.Entities)
            {
                RefcollRecords.Add(en.ToEntityReference());
            }

            return RefcollRecords;
        }
    }
}
