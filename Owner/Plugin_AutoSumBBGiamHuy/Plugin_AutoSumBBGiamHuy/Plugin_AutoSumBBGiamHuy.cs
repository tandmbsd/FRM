using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Plugin_AutoSumBBGiamHuy
{
    public class Plugin_AutoSumBBGiamHuy : IPlugin
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
            Entity target = null;


            if (context.MessageName == "Update" || context.MessageName == "Create")
            {
                target = (Entity)context.InputParameters["Target"];

                trace.Trace("start");

                if (target.Contains("new_loaibienban") || target.Contains("new_dientichgiam"))
                {
                    Entity chitietbbgiamhuy = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_bbgiamhuydientich" }));

                    Entity BBGiamHuy = service.Retrieve("new_bienbangiamhuydientich", ((EntityReference)chitietbbgiamhuy["new_bbgiamhuydientich"]).Id,
                            new ColumnSet(new string[] { "new_dientichgiamhuy", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep" }));

                    decimal dientichgiamhientai = 0;
                    Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)BBGiamHuy["new_hopdongdautumia"]).Id,
                        new ColumnSet(new string[] { "new_hopdongdautumiaid" }));

                    List<Entity> lstChitietbbgiamhuy = RetrieveMultiRecord(service, "new_chitietbbgiamhuydientich",
                        new ColumnSet(new string[] { "new_loaibienban", "new_dientichgiam","new_thuadat" }), "new_bbgiamhuydientich", BBGiamHuy.Id);

                    foreach (Entity en in lstChitietbbgiamhuy)
                    {
                        if (en.Contains("new_loaibienban"))
                        {
                            if (((OptionSetValue)en["new_loaibienban"]).Value == 100000000) // giam dien tich
                            {
                                decimal dientichgiam = en.Contains("new_dientichgiam") ? (decimal)en["new_dientichgiam"] : 0;
                                dientichgiamhientai += dientichgiam;
                            }
                            else // huy dien tich
                            {
                                Entity chitietHD = null;

                                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                                q.ColumnSet = new ColumnSet(new string[] { "new_dientichconlai" });
                                q.Criteria = new FilterExpression();
                                q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                                q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hddtm.Id));
                                q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                                EntityCollection entc = service.RetrieveMultiple(q);

                                if (entc.Entities.Count > 0)
                                    chitietHD = entc.Entities.ToList<Entity>().FirstOrDefault();

                                if (chitietHD == null)
                                    return;

                                dientichgiamhientai += chitietHD.Contains("new_dientichconlai") ? (decimal)chitietHD["new_dientichconlai"] : 0;
                            }
                        }
                    }

                    BBGiamHuy["new_dientichgiamhuy"] = dientichgiamhientai;
                    service.Update(BBGiamHuy);
                }
            }
            else
            {
                target = context.PreEntityImages["PreImg"] as Entity;

                trace.Trace("start");

                if (target.Contains("new_loaibienban") || target.Contains("new_dientichgiam"))
                {                    
                    Entity BBGiamHuy = service.Retrieve("new_bienbangiamhuydientich", ((EntityReference)target["new_bbgiamhuydientich"]).Id,
                            new ColumnSet(new string[] { "new_dientichgiamhuy", "new_hopdongdautumia", "new_khachhang", "new_khachhangdoanhnghiep" }));

                    decimal dientichgiamhientai = 0;
                    Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)BBGiamHuy["new_hopdongdautumia"]).Id,
                        new ColumnSet(new string[] { "new_hopdongdautumiaid" }));

                    List<Entity> lstChitietbbgiamhuy = RetrieveMultiRecord(service, "new_chitietbbgiamhuydientich",
                        new ColumnSet(new string[] { "new_loaibienban", "new_dientichgiam","new_thuadat" }), "new_bbgiamhuydientich", BBGiamHuy.Id);

                    foreach (Entity en in lstChitietbbgiamhuy)
                    {
                        if (en.Contains("new_loaibienban"))
                        {
                            if (((OptionSetValue)en["new_loaibienban"]).Value == 100000000) // giam dien tich
                            {
                                decimal dientichgiam = en.Contains("new_dientichgiam") ? (decimal)en["new_dientichgiam"] : 0;
                                dientichgiamhientai += dientichgiam;
                            }
                            else // huy dien tich
                            {
                                Entity chitietHD = null;

                                QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                                q.ColumnSet = new ColumnSet(new string[] { "new_dientichconlai" });
                                q.Criteria = new FilterExpression();
                                q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)en["new_thuadat"]).Id));
                                q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hddtm.Id));
                                q.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 0));
                                q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));
                                EntityCollection entc = service.RetrieveMultiple(q);

                                if (entc.Entities.Count > 0)
                                    chitietHD = entc.Entities.ToList<Entity>().FirstOrDefault();

                                if (chitietHD == null)
                                    return;

                                dientichgiamhientai += chitietHD.Contains("new_dientichconlai") ? (decimal)chitietHD["new_dientichconlai"] : 0;
                            }
                        }
                    }

                    BBGiamHuy["new_dientichgiamhuy"] = dientichgiamhientai;
                    service.Update(BBGiamHuy);
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
