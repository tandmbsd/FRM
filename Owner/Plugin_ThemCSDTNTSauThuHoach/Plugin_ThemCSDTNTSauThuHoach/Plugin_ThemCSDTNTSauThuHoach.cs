using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_ThemCSDTNTSauThuHoach
{
    public class Plugin_ThemCSDTNTSauThuHoach : IPlugin
    {
        private IOrganizationService service;
        private IOrganizationServiceFactory factory;
        private ITracingService trace;
        Entity target = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            target = context.InputParameters["Target"] as Entity;

            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

            if (!target.Contains("new_nghiemthusauthuhoach"))
                throw new Exception("Nghiệm thu sau thu hoạch không có giá trị");

            if (!target.Contains("new_hopdongthuhoach"))
                throw new Exception("Hợp đồng thu hoạch không có giá trị");

            if (!target.Contains("new_thuadat"))
                throw new Exception("Thửa đất không có giá trị");

            //if (!target.Contains("new_doitacthuhoachkh") && !target.Contains("new_doitacthuhoachkhdn"))
            //    throw new Exception("Đối tác thu hoạch không có giá trị");

            Entity nghiemthusauthuhoach = service.Retrieve("new_nghiemthuchatsatgoc", ((EntityReference)target["new_nghiemthusauthuhoach"]).Id,
                new ColumnSet(new string[] { "new_hopdongdautumia" }));

            Guid hopdongmiaID = ((EntityReference)nghiemthusauthuhoach["new_hopdongdautumia"]).Id;
            Guid hopdongthuhoachID = ((EntityReference)target["new_hopdongthuhoach"]).Id;
            Guid thuadatID = ((EntityReference)target["new_thuadat"]).Id;

            QueryExpression q = new QueryExpression("new_lenhdon");
            q.ColumnSet = new ColumnSet(new string[] { "new_chinhsachthumua", "new_name" });
            q.Criteria = new FilterExpression(LogicalOperator.And);
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongmiaID));
            q.Criteria.AddCondition(new ConditionExpression("new_hopdongthuhoach", ConditionOperator.Equal, hopdongthuhoachID));

            if (target.Contains("new_doitacthuhoachkh"))
                q.Criteria.AddCondition(new ConditionExpression("new_doitacthuhoach", ConditionOperator.Equal,
                    ((EntityReference)target["new_doitacthuhoachkh"]).Id));
            else if (target.Contains("new_doitacthuhoachkhdn"))
                q.Criteria.AddCondition(new ConditionExpression("new_doitacthuhoachkhdn", ConditionOperator.Equal,
                    ((EntityReference)target["new_doitacthuhoachkhdn"]).Id));

            LinkEntity l = new LinkEntity("new_lenhdon", "new_thuadatcanhtac", "new_thuacanhtac", "new_thuadatcanhtacid", JoinOperator.Inner);
            l.LinkCriteria = new FilterExpression();
            l.LinkCriteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, thuadatID));
            q.LinkEntities.Add(l);

            EntityCollection entc = service.RetrieveMultiple(q);

            if (entc.Entities.Count > 0)
            {
                Entity lenhdon = entc.Entities.FirstOrDefault();

                if (lenhdon.Contains("new_chinhsachthumua"))
                {
                    Entity chitietnghiemthusauthuhoach = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_chinhsachthumua", "new_dinhmuc" }));

                    Entity CSTM = service.Retrieve("new_chinhsachthumua",
                        ((EntityReference) lenhdon["new_chinhsachthumua"]).Id,new ColumnSet(true));

                    chitietnghiemthusauthuhoach["new_chinhsachthumua"] = lenhdon["new_chinhsachthumua"];
                    service.Update(chitietnghiemthusauthuhoach);
                }
            }
        }
    }
}
