using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutoSumPDNGiaiNgan
{
    public class Plugin_AutoSumPDNGiaiNgan : IPlugin
    {

        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.MessageName == "Create" || context.MessageName == "Update")
            {
                Entity target = (Entity)context.InputParameters["Target"];

                Entity ctdngn = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_phieudenghigiaingan" }));

                if (ctdngn.Contains("new_phieudenghigiaingan"))
                {
                    Entity pdngiaingan = service.Retrieve("new_phieudenghigiaingan",
                        ((EntityReference)ctdngn["new_phieudenghigiaingan"]).Id, new ColumnSet(new string[] { "new_phieudenghigiainganid" }));

                    List<Entity> lstChitietpdngiaingan = RetrieveMultiRecord(service, "new_chitietphieudenghigiaingan",
                        new ColumnSet(new string[] { "new_denghi_hoanlai_tienmat", "new_denghi_hoanlai_vattu", "new_denghi_khonghoanlai" }),
                        "new_phieudenghigiaingan", pdngiaingan.Id);

                    decimal denghihltm = 0;
                    decimal denghihlvt = 0;
                    decimal denghikhl = 0;

                    foreach (Entity en in lstChitietpdngiaingan)
                    {
                        denghihltm += en.Contains("new_denghi_hoanlai_tienmat") ? ((Money)en["new_denghi_hoanlai_tienmat"]).Value : new decimal(0);
                        denghihlvt += en.Contains("new_denghi_hoanlai_vattu") ? ((Money)en["new_denghi_hoanlai_vattu"]).Value : new decimal(0);
                        denghikhl += en.Contains("new_denghi_khonghoanlai") ? ((Money)en["new_denghi_khonghoanlai"]).Value : new decimal(0);
                    }

                    Entity pdngiainganNew = service.Retrieve("new_phieudenghigiaingan",
                        ((EntityReference)ctdngn["new_phieudenghigiaingan"]).Id, new ColumnSet(new string[] {
                            "new_sotiendtkhonghoanlai","new_sotiendthoanlai","new_sotien" }));

                    pdngiainganNew["new_sotiendthoanlai"] = new Money(denghihltm + denghihlvt);
                    pdngiainganNew["new_sotiendtkhonghoanlai"] = new Money(denghikhl);
                    pdngiainganNew["new_sotien"] = new Money(denghihltm + denghihlvt + denghikhl);


                    service.Update(pdngiainganNew);
                }
            }

            else if (context.MessageName == "Delete")
            {
                Entity fullEntity = (Entity)context.PreEntityImages["PreImg"];

                if (fullEntity.Contains("new_phieudenghigiaingan"))
                {
                    Entity pdngiaingan = service.Retrieve("new_phieudenghigiaingan",
                        ((EntityReference)fullEntity["new_phieudenghigiaingan"]).Id, new ColumnSet(new string[] { "new_phieudenghigiainganid" }));

                    List<Entity> lstChitietpdngiaingan = RetrieveMultiRecord(service, "new_chitietphieudenghigiaingan",
                        new ColumnSet(new string[] { "new_denghi_hoanlai_tienmat", "new_denghi_hoanlai_vattu", "new_denghi_khonghoanlai" }),
                        "new_phieudenghigiaingan", pdngiaingan.Id);

                    decimal denghihltm = 0;
                    decimal denghihlvt = 0;
                    decimal denghikhl = 0;

                    foreach (Entity en in lstChitietpdngiaingan)
                    {
                        denghihltm += en.Contains("new_denghi_hoanlai_tienmat") ? ((Money)en["new_denghi_hoanlai_tienmat"]).Value : new decimal(0);
                        denghihlvt += en.Contains("new_denghi_hoanlai_vattu") ? ((Money)en["new_denghi_hoanlai_vattu"]).Value : new decimal(0);
                        denghikhl += en.Contains("new_denghi_khonghoanlai") ? ((Money)en["new_denghi_khonghoanlai"]).Value : new decimal(0);
                    }

                    Entity pdngiainganNew = service.Retrieve("new_phieudenghigiaingan",
                        ((EntityReference)fullEntity["new_phieudenghigiaingan"]).Id, new ColumnSet(new string[] {
                            "new_sotiendtkhonghoanlai","new_sotiendthoanlai","new_sotien" }));

                    pdngiainganNew["new_sotiendthoanlai"] = new Money(denghihltm + denghihlvt);
                    pdngiainganNew["new_sotiendtkhonghoanlai"] = new Money(denghikhl);
                    pdngiainganNew["new_sotien"] = new Money(denghihltm + denghihlvt + denghikhl);

                    service.Update(pdngiainganNew);
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
