using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;


namespace Plugin_CheckThuadatInNTSauThuHoach
{
    public class Plugin_CheckThuadatInNTSauThuHoach : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
            {
                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("new_nghiemthusauthuhoach") && target.Contains("new_thuadat"))
                {
                    Entity ctnghiemthusauthuhoach = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    int count = 0;
                    int count1 = 0;

                    Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)ctnghiemthusauthuhoach["new_thuadat"]).Id, new ColumnSet(true));
                    Entity nghiemthusauthuhoach = service.Retrieve("new_nghiemthuchatsatgoc", ((EntityReference)ctnghiemthusauthuhoach["new_nghiemthusauthuhoach"]).Id, new ColumnSet(true));
                    Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)nghiemthusauthuhoach["new_hopdongdautumia"]).Id, new ColumnSet(true));
                    Entity vudautu = service.Retrieve("new_vudautu", ((EntityReference)hopdongdautumia["new_vudautu"]).Id, new ColumnSet(true));
                    List<Entity> lstCtnghiemthusauthuhoach = RetrieveMultiRecord(service, "new_chitietnghiemthusauthuhoach", new ColumnSet(true), "new_nghiemthusauthuhoach", nghiemthusauthuhoach.Id);
                    string status = ((OptionSetValue)nghiemthusauthuhoach["statuscode"]).Value.ToString();

                    foreach (Entity en in lstCtnghiemthusauthuhoach)
                    {
                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                            count++;
                    }

                    if (count > 1)
                        throw new Exception("Thửa đất đã tồn tại trong chi tiết nghiệm thu chặt sát gốc  khác !!!");

                    QueryExpression q = new QueryExpression("new_chitietnghiemthusauthuhoach");
                    q.ColumnSet = new ColumnSet(new string[] { "new_nghiemthusauthuhoach", "new_thuadat", "new_name" });
                    LinkEntity linkEntity1 = new LinkEntity("new_chitietnghiemthusauthuhoach", "new_nghiemthuchatsatgoc", "new_nghiemthusauthuhoach", "activityid", JoinOperator.Inner);
                    LinkEntity linkEntity2 = new LinkEntity("new_nghiemthuchatsatgoc", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                    q.LinkEntities.Add(linkEntity1);
                    linkEntity1.LinkCriteria = new FilterExpression();
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    linkEntity2.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, vudautu.Id));
                    q.LinkEntities.Add(linkEntity2);
                    EntityCollection entc = service.RetrieveMultiple(q);
                    List<Entity> lstCtnghiemthusauthuhoach1 = entc.Entities.ToList();

                    foreach (Entity en in lstCtnghiemthusauthuhoach1)
                    {
                        Entity ntsth = service.Retrieve("new_nghiemthuchatsatgoc", ((EntityReference)en["new_nghiemthusauthuhoach"]).Id, new ColumnSet(true));

                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                            count1++;

                    }

                    if (status == "100000000")
                    {
                        if (count1 > 1)
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu chặt sát gốc khác !!!");

                    }
                    else
                    {
                        if (count1 > 0)
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu chặt sát gốc khác !!!");

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
