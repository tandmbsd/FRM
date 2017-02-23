using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace TongGTTaiSan_DeNghiRutHoSo
{
    public class TongGTTaiSan_DeNghiRutHoSo : IPlugin
    {
        private IOrganizationService service;
        private IOrganizationServiceFactory serviceProxy;

        public void Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            serviceProxy = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = serviceProxy.CreateOrganizationService(context.UserId);
            var trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = null;
            EntityReference Enftarget = null;
            
            if (context.MessageName != "Delete")
            {
                target = (Entity)context.InputParameters["Target"];
            }

            if (target != null && target.LogicalName == "new_denghiruthoso" && context.Depth < 2)
            {
                trace.Trace("A");
                if (target.Contains("new_tinhtrangduyet") && ((OptionSetValue)target["new_tinhtrangduyet"]).Value == 100000007)
                {
                    List<Entity> lstCtdenghi = RetrieveMultiRecord(service,"new_chitietdenghiruthoso",
                             new ColumnSet(new string[] { "statuscode" }),
                             "new_denghiruthoso", target.Id);

                    foreach (Entity en in lstCtdenghi)
                    {
                        en["statuscode"] = new OptionSetValue(100000001); // rut
                        service.Update(en);
                    }
                }

                EntityReference nguoidenghi = null;
                decimal tongtien2ben = 0;
                decimal tongtien3ben = 0;

                if (target.Contains("new_nguoidenghikh"))
                    nguoidenghi = (EntityReference)target["new_nguoidenghikh"];
                else if (target.Contains("new_nguoidenghikhdn"))
                    nguoidenghi = (EntityReference)target["new_nguoidenghikhdn"];

                if (nguoidenghi != null)
                {
                    QueryExpression q = new QueryExpression("new_hopdongthechap");
                    q.ColumnSet = new ColumnSet(true);
                    q.Criteria = new FilterExpression();
                    q.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

                    if (nguoidenghi.LogicalName == "contact") // kh
                        q.Criteria.AddCondition(new ConditionExpression("new_chuhopdong", ConditionOperator.Equal, nguoidenghi.Id));
                    else
                        q.Criteria.AddCondition(new ConditionExpression("new_chuhopdongdoanhnghiep", ConditionOperator.Equal, nguoidenghi.Id));
                    q.Criteria.AddCondition(new ConditionExpression("new_benthuba", ConditionOperator.Equal, false));

                    EntityCollection entc = service.RetrieveMultiple(q);

                    foreach (Entity en in entc.Entities)
                    {
                        tongtien2ben += Tinhtongtaisanfromhdthechap(en);
                    }

                    QueryExpression q1 = new QueryExpression("new_hopdongthechap");
                    q1.ColumnSet = new ColumnSet(true);
                    q1.Criteria = new FilterExpression();
                    q1.Criteria.AddCondition(new ConditionExpression("statuscode", ConditionOperator.Equal, 100000000));

                    if (nguoidenghi.LogicalName == "contact") // kh
                        q1.Criteria.AddCondition(new ConditionExpression("new_nguoidambaochinhkhcn", ConditionOperator.Equal, nguoidenghi.Id));
                    else
                        q1.Criteria.AddCondition(new ConditionExpression("new_nguoiduocdambaochinhkhdn", ConditionOperator.Equal, nguoidenghi.Id));
                    q1.Criteria.AddCondition(new ConditionExpression("new_benthuba", ConditionOperator.Equal, true));

                    EntityCollection entc1 = service.RetrieveMultiple(q1);

                    foreach (Entity en in entc1.Entities)
                    {
                        tongtien3ben += Tinhtongtaisanfromhdthechap(en);
                    }

                    Entity updateDenghi = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_tonggiatritaisandangthechap" }));

                    updateDenghi["new_tonggiatritaisandangthechap"] = new Money(tongtien2ben + tongtien3ben);
                    service.Update(updateDenghi);

                }
            }
            else if (context.MessageName == "Delete" || (target != null && target.LogicalName == "new_chitietdenghiruthoso"))
            {
                trace.Trace("B");
                if (context.MessageName == "Create" || context.MessageName == "Update")
                {
                    Entity ctdenghiruthoso = service.Retrieve(target.LogicalName, target.Id,
                        new ColumnSet(new string[] { "new_denghiruthoso" }));
                    decimal gtdenghirut = 0;

                    if (ctdenghiruthoso.Contains("new_denghiruthoso"))
                    {

                        Entity dnruthoso = service.Retrieve("new_denghiruthoso", ((EntityReference)ctdenghiruthoso["new_denghiruthoso"]).Id,
                            new ColumnSet(new string[] { "new_tonggiatritaisandangthechap", "new_giatridenghirut", "new_giatritstcconlai" }));

                        List<Entity> lstCtdenghi = RetrieveMultiRecord(service, target.LogicalName,
                            new ColumnSet(new string[] { "new_giatrithechap" }),
                            "new_denghiruthoso", dnruthoso.Id);

                        foreach (Entity en in lstCtdenghi)
                        {
                            gtdenghirut += en.Contains("new_giatrithechap") ? ((Money)en["new_giatrithechap"]).Value : 0;
                        }

                        decimal tonggttaisanthechap = dnruthoso.Contains("new_tonggiatritaisandangthechap") ?
                            ((Money)dnruthoso["new_tonggiatritaisandangthechap"]).Value : 0;
                        decimal gtthevao = dnruthoso.Contains("new_giatritaisanthevao")
                            ? (decimal)dnruthoso["new_giatritaisanthevao"]
                            : 0;
                        decimal gtconlai = tonggttaisanthechap - gtdenghirut + gtthevao;

                        if (gtconlai < 0)
                            throw new Exception("Giá trị đề nghị rút đã vượt quá tổng gt tài sản thế chấp");

                        dnruthoso["new_giatridenghirut"] = new Money(gtdenghirut);
                        dnruthoso["new_giatritstcconlai"] = new Money(gtconlai);

                        service.Update(dnruthoso);
                    }
                }
                else if (context.MessageName == "Delete")
                {
                    trace.Trace("C");
                    Entity fullEntity = (Entity)context.PreEntityImages["PreImg"];

                    if (fullEntity.Contains("new_denghiruthoso"))
                    {
                        Entity dnruthoso = service.Retrieve("new_denghiruthoso", ((EntityReference)fullEntity["new_denghiruthoso"]).Id,
                            new ColumnSet(new string[] { "new_tonggiatritaisandangthechap", "new_giatridenghirut", "new_giatritstcconlai" }));
                        decimal gtdenghirut = 0;

                        List<Entity> lstCtdenghi = RetrieveMultiRecord(service, fullEntity.LogicalName,
                            new ColumnSet(new string[] { "new_giatrithechap" }),
                            "new_denghiruthoso", dnruthoso.Id);
                        
                        foreach (Entity en in lstCtdenghi)
                        {
                            gtdenghirut += en.Contains("new_giatrithechap") ? ((Money)en["new_giatrithechap"]).Value : 0;
                        }

                        decimal tonggttaisanthechap = dnruthoso.Contains("new_tonggiatritaisandangthechap") ?
                            ((Money)dnruthoso["new_tonggiatritaisandangthechap"]).Value : 0;
                        decimal gtconlai = tonggttaisanthechap - gtdenghirut;

                        if (gtconlai < 0)
                            throw new Exception("Giá trị đề nghị rút đã vượt quá tổng gt tài sản thế chấp");

                        dnruthoso["new_giatridenghirut"] = new Money(gtdenghirut);
                        dnruthoso["new_giatritstcconlai"] = new Money(gtconlai);

                        service.Update(dnruthoso);
                    }
                }
            }
            //throw new Exception("abc");
        }

        private List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column,
            string condition, object value)
        {
            var q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            var entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList();
        }

        decimal Tinhtongtaisanfromhdthechap(Entity hdtc)
        {
            decimal result = 0;
            List<Entity> lstTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                        new ColumnSet(new string[] { "new_giatridinhgiagiatrithechap" }), "new_hopdongthechap", hdtc.Id);

            foreach (Entity en in lstTaisanthechap)
            {
                result += en.Contains("new_giatridinhgiagiatrithechap")
                    ? ((Money)en["new_giatridinhgiagiatrithechap"]).Value
                    : 0;
            }

            return result;
        }
    }
}