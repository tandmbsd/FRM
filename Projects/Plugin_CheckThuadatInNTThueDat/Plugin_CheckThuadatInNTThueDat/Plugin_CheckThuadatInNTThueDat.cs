using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckThuadatInNTThueDat
{
    //moi nhat
    public class Plugin_CheckThuadatInNTThueDat : IPlugin
    {
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

                if (target.Contains("new_thuadat"))
                {
                    Entity chitietnghiemthuthuedat = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    int count = 0;
                    int count1 = 0;

                    Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)chitietnghiemthuthuedat["new_thuadat"]).Id, new ColumnSet(new string[] {"new_thuadatid"}));
                    Entity nghiemthuthuedat = service.Retrieve("new_nghiemthuthuedat", ((EntityReference)chitietnghiemthuthuedat["new_nghiemthuthuedat"]).Id, new ColumnSet(new string[] {"statuscode","new_vudautu", "new_lannghiemthu_global" }));
                    
                    List<Entity> lstChitietnghiemthuthuedat = RetrieveMultiRecord(service, "new_chitietnghiemthuthuedat", new ColumnSet(new string[] {"new_thuadat"}), "new_nghiemthuthuedat", nghiemthuthuedat.Id);
                    string status = ((OptionSetValue)nghiemthuthuedat["statuscode"]).Value.ToString();

                    foreach (Entity en in lstChitietnghiemthuthuedat)
                    {
                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                        {
                            count++;
                        }
                    }

                    if (count > 1)
                    {
                        //throw new Exception(nghiemthusauthuhoach["subject"].ToString() + count.ToString());
                        throw new Exception("Thửa đất đã tồn tại trong chi tiết nghiệm thu thuê đất  khác !!!");
                    }

                    QueryExpression q = new QueryExpression("new_chitietnghiemthuthuedat");
                    q.ColumnSet = new ColumnSet(new string[] { "new_nghiemthuthuedat", "new_thuadat", "new_name" });
                    LinkEntity linkEntity1 = new LinkEntity("new_chitietnghiemthuthuedat", "new_nghiemthuthuedat", "new_nghiemthuthuedat", "new_nghiemthuthuedatid", JoinOperator.Inner);

                    q.LinkEntities.Add(linkEntity1);
                    linkEntity1.LinkCriteria = new FilterExpression();
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_lannghiemthu_global", ConditionOperator.Equal, ((OptionSetValue)nghiemthuthuedat["new_lannghiemthu_global"]).Value));
                    linkEntity1.LinkCriteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal, ((EntityReference)nghiemthuthuedat["new_vudautu"]).Id));
                    EntityCollection entc = service.RetrieveMultiple(q);
                    List<Entity> lstChitietnghiemthuthuedat1 = entc.Entities.ToList();

                    foreach (Entity en in lstChitietnghiemthuthuedat1)
                    {
                        if (thuadat.Id == ((EntityReference)en["new_thuadat"]).Id)
                        {
                            count1++;
                        }
                    }

                    if (status == "100000000")
                    {
                        if (count1 > 1)
                        {
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu thuê đất khác !!!");
                        }
                    }
                    else
                    {
                        if (count1 > 0)
                        {
                            throw new Exception("Thửa đất đã tồn tại trong nghiệm thu thuê đất khác !!!");
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
    }
}
