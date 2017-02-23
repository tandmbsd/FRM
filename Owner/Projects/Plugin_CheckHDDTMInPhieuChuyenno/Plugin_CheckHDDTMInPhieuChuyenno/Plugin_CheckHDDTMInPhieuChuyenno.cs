using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckHDDTMInPhieuChuyenno
{
    public class Plugin_CheckHDDTMInPhieuChuyenno : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));

            service = factory.CreateOrganizationService(context.UserId);

            EntityReference targetEntity = null;
            string relationshipName = string.Empty;
            EntityReferenceCollection relatedEntities = null;
            EntityReference relatedEntity = null;

            Entity phieuchuyenno = new Entity();
            Entity hopdongdautumia = new Entity();
            bool flag = true;

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                if (context.InputParameters.Contains("Relationship"))
                {
                    relationshipName = context.InputParameters["Relationship"].ToString();
                }

                if (relationshipName != "new_new_phieuchuyenno_new_hopdongdautumia.")
                {
                    return;
                }

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    phieuchuyenno = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_khachhang_bena", "new_khachhangdoanhnghiep_bena", "new_name" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    if (relatedEntities.Count > 0)
                    {
                        relatedEntity = relatedEntities[0];
                        hopdongdautumia = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_name" }));
                    }
                    else
                    {
                        return;
                    }
                }

                if ((!phieuchuyenno.Contains("new_khachhang_bena")) && (!hopdongdautumia.Contains("new_khachhang")) && (!phieuchuyenno.Contains("new_khachhangdoanhnghiep_bena")) && (!hopdongdautumia.Contains("new_khachhangdoanhnghiep")))
                {
                    //throw new Exception("Hợp đồng đầu tư mía " + hopdongdautumia["new_name"].ToString() + " không hợp lệ");
                    flag = false;
                }

                if (phieuchuyenno.Contains("new_khachhang_bena"))
                {
                    if (hopdongdautumia.Contains("new_khachhangdoanhnghiep"))
                    {
                        //throw new Exception("Hợp đồng đầu tư mía " + hopdongdautumia["new_name"].ToString() + " không hợp lệ");
                        flag = false;
                    }
                    else if (hopdongdautumia.Contains("new_khachhang"))
                    {
                        if (((EntityReference)phieuchuyenno["new_khachhang_bena"]).Id != ((EntityReference)hopdongdautumia["new_khachhang"]).Id)
                        {
                            //throw new Exception("Hợp đồng đầu tư mía " + hopdongdautumia["new_name"].ToString() + " không hợp lệ");
                            flag = false;
                        }
                    }
                }

                else if (phieuchuyenno.Contains("new_khachhangdoanhnghiep_bena"))
                {
                    if (hopdongdautumia.Contains("new_khachhang"))
                    {
                        //throw new Exception("Hợp đồng đầu tư mía " + hopdongdautumia["new_name"].ToString() + " không hợp lệ");
                        flag = false;
                    }
                    else if (hopdongdautumia.Contains("new_khachhangdoanhnghiep"))
                    {
                        if (((EntityReference)phieuchuyenno["new_khachhangdoanhnghiep_bena"]).Id != ((EntityReference)hopdongdautumia["new_khachhangdoanhnghiep"]).Id)
                        {
                            //throw new Exception("Hợp đồng đầu tư mía " + hopdongdautumia["new_name"].ToString() + " không hợp lệ");
                            flag = false;
                        }
                    }
                }

                if (flag == true)
                {
                    List<Entity> lstChitiet = RetrieveMultiRecord(service, "new_thuadatcanhtac", new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_name" }), "new_hopdongdautumia", hopdongdautumia.Id);

                    foreach (Entity en in lstChitiet)
                    {
                        Entity pcn_chitiet = new Entity("new_phieuchuyennochitiethddtmia");
                        pcn_chitiet["new_name"] = en["new_name"].ToString() + "-" + phieuchuyenno["new_name"].ToString();
                        pcn_chitiet["new_phieuchuyenno"] = phieuchuyenno.ToEntityReference();
                        pcn_chitiet["new_chitiethddtmia"] = en.ToEntityReference();
                        pcn_chitiet["new_hinhthucchuyen"] = new OptionSetValue(100000000); // chuyển toàn bộ
                        service.Create(pcn_chitiet);
                    }
                }
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
