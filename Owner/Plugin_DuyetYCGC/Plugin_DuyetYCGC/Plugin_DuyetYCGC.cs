using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_DuyetYCGC
{
    public class Plugin_DuyetYCGC : IPlugin
    {
        //moi nhất và gộp Plugin_DuyetYeuCauGiaiChap
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                // khi có yêu cầu giải chấp thi cập nhập tài sản là chưa thế chấp và tình trang TSTC là giải chấp
                Entity ycgc = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_hopdauthechap", "new_taisan", "new_ngaygiaichap" }));

                Entity taisan = service.Retrieve("new_taisan", ((EntityReference)ycgc["new_taisan"]).Id,
                    new ColumnSet(new string[] { "new_trangthaitaisan" }));
                taisan["new_trangthaitaisan"] = new OptionSetValue(100000000); // chua the chap

                service.Update(taisan);

                QueryExpression qTaisanthechap = new QueryExpression("new_taisanthechap");
                qTaisanthechap.ColumnSet = new ColumnSet(true);
                qTaisanthechap.Criteria = new FilterExpression();
                qTaisanthechap.Criteria.AddCondition(new ConditionExpression("new_hopdongthechap", ConditionOperator.Equal, ((EntityReference)ycgc["new_hopdauthechap"]).Id));
                qTaisanthechap.Criteria.AddCondition(new ConditionExpression("new_taisan", ConditionOperator.Equal, ((EntityReference)ycgc["new_taisan"]).Id));
                EntityCollection entcTSTC = service.RetrieveMultiple(qTaisanthechap);

                foreach (Entity en in entcTSTC.Entities)
                {
                    Entity k = service.Retrieve(en.LogicalName, en.Id, new ColumnSet(new string[] { "statuscode","new_name" }));
                    //throw new Exception(k["new_name"].ToString());
                    k["statuscode"] = new OptionSetValue(100000001); // giai chap
                    
                    service.Update(k);
                    
                    QueryExpression qCT = new QueryExpression("new_chitietdenghiruthoso");
                    qCT.ColumnSet = new ColumnSet(new string[] { "statuscode", "new_name" });
                    qCT.Criteria = new FilterExpression();
                    qCT.Criteria.AddCondition(new ConditionExpression("new_hopdongthechap", ConditionOperator.Equal,
                        ((EntityReference)ycgc["new_hopdauthechap"]).Id));
                    qCT.Criteria.AddCondition(new ConditionExpression("new_taisanthechap", ConditionOperator.Equal, en.Id));
                    EntityCollection entcCT = service.RetrieveMultiple(qCT);

                    foreach (Entity ct in entcCT.Entities)
                    {
                        Entity t = new Entity(ct.LogicalName);
                        t.Id = ct.Id;
                        t["statuscode"] = new OptionSetValue(100000001);
                        service.Update(t);
                    }

                }

                // plug gộp (nếu là giải chấp hết thì cập nhập hợp dồng là thanh lý)
                List<Entity> Lst_yeucaugiaichap = RetrieveMultiRecord(service, "new_yeucaugiaichap",
                    new ColumnSet(new string[] { "new_taisan", "new_hopdauthechap" }), "new_hopdauthechap",
                    ((EntityReference)ycgc["new_hopdauthechap"]).Id);

                Entity newYCGC = service.Retrieve(ycgc.LogicalName, ycgc.Id, new ColumnSet(new string[] { "new_ngaygiaichap" }));
                newYCGC["new_ngaygiaichap"] = DateTime.Now;

                QueryExpression q = new QueryExpression("new_yeucaugiaichap");
                q.ColumnSet = new ColumnSet(new string[] { "new_taisan", "new_hopdauthechap" });
                q.Criteria = new FilterExpression(LogicalOperator.And);
                q.Criteria.AddCondition(new ConditionExpression("new_hopdauthechap", ConditionOperator.Equal, ((EntityReference)ycgc["new_hopdauthechap"]).Id));
                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                q.AddOrder("new_ngaygiaichap", OrderType.Descending);
                EntityCollection entc = service.RetrieveMultiple(q);

                int sltaisangiaichap = entc.Entities.Count;

                Entity hopdongthechap = service.Retrieve("new_hopdongthechap", ((EntityReference)ycgc["new_hopdauthechap"]).Id, new ColumnSet(new string[] { "statuscode" }));
                List<Entity> DStaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap", new ColumnSet(true), "new_hopdongthechap", hopdongthechap.Id);
                
                if (sltaisangiaichap == DStaisanthechap.Count)
                {
                    hopdongthechap["statuscode"] = new OptionSetValue(100000001); // thanh ly
                    service.Update(hopdongthechap);
                }
                                
                service.Update(newYCGC);
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
