using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckThuaDat_NghiemThuTuoiMia
{
    public class CheckThuaDat_NghiemThuTuoiMia : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
            {
                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("new_thuadat") && target["new_thuadat"] != null)
                {
                    Entity chitietnghiemthutuoimia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    int count = 0;
                    int count1 = 0;

                    Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)chitietnghiemthutuoimia["new_thuadat"]).Id, new ColumnSet(new string[] { "new_thuadatid" }));
                    Entity nghiemthutuoimia = service.Retrieve("new_nghiemthutuoimia", ((EntityReference)chitietnghiemthutuoimia["new_nghiemthutuoimia"]).Id, new ColumnSet(new string[] { "statuscode", "new_hopdongtrongmia", "new_lannghiemthu_global" }));
                    Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)nghiemthutuoimia["new_hopdongtrongmia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));
                    trace.Trace("1");
                    List<Entity> lstChitietnghiemthutuoimia = RetrieveMultiRecord(service, "new_chitietnghiemthutuoimia", new ColumnSet(true), "new_nghiemthutuoimia", nghiemthutuoimia.Id);
                    string status = ((OptionSetValue)nghiemthutuoimia["statuscode"]).Value.ToString();
                    trace.Trace("2");
                    foreach (Entity en in lstChitietnghiemthutuoimia)
                    {
                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                        {
                            count++;
                        }
                    }
                    trace.Trace("3");
                    if (count > 1 && CheckThuadathoanthanh(thuadat, hddtm) == false)
                    {
                        //throw new Exception(nghiemthusauthuhoach["subject"].ToString() + count.ToString());
                        throw new Exception("Thửa đất đã tồn tại trong chi tiết nghiệm thu tưới mía khác !!!");
                    }

                    QueryExpression q = new QueryExpression("new_chitietnghiemthutuoimia");
                    q.ColumnSet = new ColumnSet(new string[] { "new_nghiemthutuoimia", "new_thuadat", "new_name" });
                    LinkEntity linkEntity1 = new LinkEntity("new_chitietnghiemthutuoimia", "new_nghiemthutuoimia", "new_nghiemthutuoimia", "activityid", JoinOperator.Inner);
                    LinkEntity linkEntity2 = new LinkEntity("new_nghiemthutuoimia", "new_hopdongdautumia", "new_hopdongtrongmia", "new_hopdongdautumiaid", JoinOperator.Inner);
                    trace.Trace("4");
                    linkEntity1.LinkEntities.Add(linkEntity2);
                    q.LinkEntities.Add(linkEntity1);
                    linkEntity1.LinkCriteria = new FilterExpression();
                    linkEntity2.LinkCriteria = new FilterExpression();
                    trace.Trace("4");
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    trace.Trace("5");
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_lannghiemthu_global", ConditionOperator.Equal, ((OptionSetValue)nghiemthutuoimia["new_lannghiemthu_global"]).Value));
                    trace.Trace("6");
                    linkEntity2.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)hddtm["new_vudautu"]).Id));
                    trace.Trace("7");
                    EntityCollection entc = service.RetrieveMultiple(q);
                    List<Entity> lstChitietnghiemthuthuedat1 = entc.Entities.ToList();
                    trace.Trace("8");
                    foreach (Entity en in lstChitietnghiemthuthuedat1)
                    {
                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                        {
                            count1++;
                        }
                    }
                    trace.Trace("7");
                    if (status == "100000000")
                    {
                        if (count1 > 1 && CheckThuadathoanthanh(thuadat, hddtm) == false)
                        {
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu tưới mía khác !!!");
                        }
                    }
                    else
                    {
                        if (count1 > 0 && CheckThuadathoanthanh(thuadat, hddtm) == false)
                        {
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu tưới mía khác !!!");
                        }
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

        bool CheckThuadathoanthanh(Entity thuadat, Entity hddtm)
        {
            QueryExpression q = new QueryExpression("new_thuadatcanhtac"); //100000007
            q.ColumnSet = new ColumnSet(true);
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hddtm.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, thuadat.Id));
            q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000007));
            EntityCollection entc = service.RetrieveMultiple(q);

            return (entc.Entities.Count > 0);
        }
    }
}
