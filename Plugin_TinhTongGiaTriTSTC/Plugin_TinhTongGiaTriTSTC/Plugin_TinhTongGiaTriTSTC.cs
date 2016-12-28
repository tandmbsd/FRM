using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Plugin_TinhTongGiaTriTSTC
{
    public class Plugin_TinhTongGiaTriTSTC : IPlugin
    {
        // moi nhat , đã gọp plugin UpdateTSTCFromHDThechap vào
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode")) // hop dong the chap da ky
            {
                Entity hopdongthechap = service.Retrieve(target.LogicalName, target.Id,
                   new ColumnSet(new string[] { "new_chuhopdong", "new_chuhopdongdoanhnghiep",
                       "new_benthuba", "new_nguoidambaochinhkhcn","new_nguoiduocdambaochinhkhdn","new_tonggiatrihd" }));

                bool hopdong3ben = hopdongthechap.Contains("new_benthuba") ? (bool)hopdongthechap["new_benthuba"] : false;
                decimal sumgiatrithechap = 0;

                Entity khachhang = null;

                if (hopdong3ben == false)
                {
                    if (hopdongthechap.Contains("new_chuhopdong"))
                    {
                        khachhang = service.Retrieve("contact", ((EntityReference)hopdongthechap["new_chuhopdong"]).Id,
                            new ColumnSet("new_tonggiatritstc"));

                        sumgiatrithechap = khachhang.Contains("new_tonggiatritstc") ? ((Money)khachhang["new_tonggiatritstc"]).Value : 0;
                    }
                    else if (hopdongthechap.Contains("new_chuhopdongdoanhnghiep"))
                    {
                        khachhang = service.Retrieve("account", ((EntityReference)hopdongthechap["new_chuhopdongdoanhnghiep"]).Id,
                            new ColumnSet("new_tonggiatritstc"));

                        sumgiatrithechap = khachhang.Contains("new_tonggiatritstc") ? ((Money)khachhang["new_tonggiatritstc"]).Value : 0;
                    }
                }
                else
                {
                    if (hopdongthechap.Contains("new_nguoidambaochinhkhcn"))
                    {
                        khachhang = service.Retrieve("contact", ((EntityReference)hopdongthechap["new_nguoidambaochinhkhcn"]).Id,
                            new ColumnSet("new_tonggiatritstc", "fullname"));

                        sumgiatrithechap = khachhang.Contains("new_tonggiatritstc") ? ((Money)khachhang["new_tonggiatritstc"]).Value : 0;
                    }
                    else if (hopdongthechap.Contains("new_nguoiduocdambaochinhkhdn"))
                    {
                        khachhang = service.Retrieve("account", ((EntityReference)hopdongthechap["new_nguoiduocdambaochinhkhdn"]).Id,
                            new ColumnSet("new_tonggiatritstc"));

                        sumgiatrithechap = khachhang.Contains("new_tonggiatritstc") ? ((Money)khachhang["new_tonggiatritstc"]).Value : 0;
                    }
                }

                List<Entity> DSTaisanthechap = RetrieveMultiRecord(service, "new_taisanthechap",
                    new ColumnSet(new string[] { "new_giatridinhgiagiatrithechap", "statuscode" }), "new_hopdongthechap", target.Id);

                if (((OptionSetValue)target["statuscode"]).Value == 100000000) // da ky
                {
                    foreach (Entity en in DSTaisanthechap)
                    {
                        if (en.Contains("new_giatridinhgiagiatrithechap"))                        
                            sumgiatrithechap += ((Money)en["new_giatridinhgiagiatrithechap"]).Value;                        

                        //Entity k = new Entity(en.LogicalName);
                        //k.Id = en.Id;
                        //k["statuscode"] = new OptionSetValue(100000000); //the chap
                        //service.Update(k);
                    }

                    khachhang["new_tonggiatritstc"] = new Money(sumgiatrithechap);
                }
                else if (((OptionSetValue)target["statuscode"]).Value == 100000001 || ((OptionSetValue)target["statuscode"]).Value == 100000002) // thanh ly hoac huy
                {
                    decimal giatrithechaptrenHD = hopdongthechap.Contains("new_tonggiatrihd") ? ((Money)hopdongthechap["new_tonggiatrihd"]).Value : new decimal(0);

                    if (sumgiatrithechap >= giatrithechaptrenHD)
                        sumgiatrithechap = sumgiatrithechap - giatrithechaptrenHD;

                    khachhang["new_tonggiatritstc"] = new Money(sumgiatrithechap);

                    if (((OptionSetValue)target["statuscode"]).Value == 100000001) // thanh ly
                    {
                        foreach (Entity en in DSTaisanthechap)
                        {
                            Entity k = new Entity(en.LogicalName);
                            k.Id = en.Id;
                            k["statuscode"] = new OptionSetValue(100000001); // giai chap
                            service.Update(k);
                        }
                    }
                }

                service.Update(khachhang);
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
