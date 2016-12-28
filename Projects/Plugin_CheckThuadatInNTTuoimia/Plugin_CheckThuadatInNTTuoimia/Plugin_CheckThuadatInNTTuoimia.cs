using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckThuadatInNTTuoimia
{
    public class Plugin_CheckThuadatInNTTuoimia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            // moi nhat
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
            {
                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("new_thuadat") && target.LogicalName == "new_chitietnghiemthutrongmia")
                {
                    Entity chitietnghiemthutrongmia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    int count = 0;
                    int count1 = 0;

                    if (!chitietnghiemthutrongmia.Contains("new_thuadat"))
                        return;

                    Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)chitietnghiemthutrongmia["new_thuadat"]).Id, new ColumnSet(new string[] { "new_thuadatid" }));
                    Entity nghiemthutrongmia = service.Retrieve("new_nghiemthutrongmia", ((EntityReference)chitietnghiemthutrongmia["new_nghiemthutrongmia"]).Id,
                        new ColumnSet(new string[] { "statuscode", "new_hopdongtrongmia", "new_lannghiemthu_global" }));
                    Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)nghiemthutrongmia["new_hopdongtrongmia"]).Id, new ColumnSet(new string[] { "new_vudautu" }));

                    List<Entity> lstChitietnghiemthutrongmia = RetrieveMultiRecord(service, "new_chitietnghiemthutrongmia", new ColumnSet(true), "new_nghiemthutrongmia", nghiemthutrongmia.Id);
                    string status = ((OptionSetValue)nghiemthutrongmia["statuscode"]).Value.ToString();

                    foreach (Entity en in lstChitietnghiemthutrongmia)
                    {
                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                            count++;
                    }

                    if (count > 1 && CheckThuadathoanthanh(thuadat, hddtm) == false)
                        throw new Exception("Thửa đất đã tồn tại trong chi tiết nghiệm thu trồng mía khác !!!");

                    QueryExpression q = new QueryExpression("new_chitietnghiemthutrongmia");
                    q.ColumnSet = new ColumnSet(new string[] { "new_nghiemthutrongmia", "new_thuadat", "new_name", "new_nghiemthutrongmia" });
                    LinkEntity linkEntity1 = new LinkEntity("new_chitietnghiemthutrongmia", "new_nghiemthutrongmia", "new_nghiemthutrongmia", "activityid", JoinOperator.Inner);
                    LinkEntity linkEntity2 = new LinkEntity("new_nghiemthutrongmia", "new_hopdongdautumia", "new_hopdongtrongmia", "new_hopdongdautumiaid", JoinOperator.Inner);

                    if (!nghiemthutrongmia.Contains("new_lannghiemthu_global"))
                        return;

                    linkEntity1.LinkEntities.Add(linkEntity2);
                    q.LinkEntities.Add(linkEntity1);
                    linkEntity1.LinkCriteria = new FilterExpression();
                    linkEntity2.LinkCriteria = new FilterExpression();
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_lannghiemthu_global", ConditionOperator.Equal, ((OptionSetValue)nghiemthutrongmia["new_lannghiemthu_global"]).Value));
                    linkEntity2.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)hddtm["new_vudautu"]).Id));
                    EntityCollection entc = service.RetrieveMultiple(q);
                    List<Entity> lstChitietnghiemthuthuedat1 = entc.Entities.ToList();

                    foreach (Entity en in lstChitietnghiemthuthuedat1)
                    {
                        if (en.Contains("new_thuadat"))
                        {
                            if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)                            
                                count1++;                            
                        }
                    }

                    if (status == "100000000")
                    {
                        if (count1 > 1 && CheckThuadathoanthanh(thuadat, hddtm) == false)                        
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu trồng mía khác !!!");                        
                    }
                    else
                    {
                        if (count1 > 0 && CheckThuadathoanthanh(thuadat, hddtm) == false)                        
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu trồng mía khác !!!");
                        
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
            q.Criteria = new FilterExpression(LogicalOperator.And);
            //q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hddtm.Id));
            q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, thuadat.Id));
            FilterExpression fl = new FilterExpression(LogicalOperator.Or);
            fl.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000007)); // thanh ly
            fl.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000005)); // huy
            q.Criteria.AddFilter(fl);
            EntityCollection entc = service.RetrieveMultiple(q);

            return (entc.Entities.Count > 0);
        }
    }
}
