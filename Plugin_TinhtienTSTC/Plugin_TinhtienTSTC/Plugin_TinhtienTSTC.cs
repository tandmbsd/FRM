using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Plugin_TinhtienTSTC
{
    public class Plugin_TinhtienTSTC : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode"))
            {
                Entity taisanthechap = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_hopdongthechap", "new_giatridinhgiagiatrithechap" }));
                decimal giatritstc = taisanthechap.Contains("new_giatridinhgiagiatrithechap") ? ((Money)taisanthechap["new_giatridinhgiagiatrithechap"]).Value : new decimal(0);

                Entity hopdongthechap = service.Retrieve("new_hopdongthechap", ((EntityReference)taisanthechap["new_hopdongthechap"]).Id,
                    new ColumnSet(new string[] { "statuscode", "new_benthuba",
                        "new_nguoidambaochinhkhcn", "new_nguoiduocdambaochinhkhdn","new_chuhopdong","new_chuhopdongdoanhnghiep" }));

                bool hopdong3ben = hopdongthechap.Contains("new_benthuba") ? (bool)hopdongthechap["new_benthuba"] : false;
                traceService.Trace(hopdong3ben.ToString());
                if (((OptionSetValue)hopdongthechap["statuscode"]).Value == 100000000) // da ky
                {
                    Entity khachhang = null;
                    decimal tonggiatritstc = 0;

                    if (((OptionSetValue)target["statuscode"]).Value == 100000001) // giai chap
                    {
                        traceService.Trace("chay if giai chap");
                        if (hopdong3ben == false)
                        {
                            traceService.Trace("1");
                            if (hopdongthechap.Contains("new_chuhopdong"))
                            {
                                khachhang = service.Retrieve("contact", ((EntityReference)hopdongthechap["new_chuhopdong"]).Id,
                                    new ColumnSet("new_tonggiatritstc", "fullname"));
                            }
                            else if (hopdongthechap.Contains("new_chuhopdongdoanhnghiep"))
                            {
                                khachhang = service.Retrieve("account", ((EntityReference)hopdongthechap["new_chuhopdongdoanhnghiep"]).Id,
                                    new ColumnSet("new_tonggiatritstc"));
                            }
                        }
                        else
                        {
                            traceService.Trace("2");
                            if (hopdongthechap.Contains("new_nguoidambaochinhkhcn"))
                            {
                                khachhang = service.Retrieve("contact", ((EntityReference)hopdongthechap["new_nguoidambaochinhkhcn"]).Id,
                                    new ColumnSet("new_tonggiatritstc", "fullname"));

                                traceService.Trace("3");
                            }
                            else if (hopdongthechap.Contains("new_nguoiduocdambaochinhkhdn"))
                            {
                                khachhang = service.Retrieve("account", ((EntityReference)hopdongthechap["new_nguoiduocdambaochinhkhdn"]).Id,
                                    new ColumnSet("new_tonggiatritstc"));

                                traceService.Trace("4");
                            }
                        }

                        tonggiatritstc = khachhang.Contains("new_tonggiatritstc") ? ((Money)khachhang["new_tonggiatritstc"]).Value : new decimal(0);

                        if (tonggiatritstc >= giatritstc)
                        {
                            khachhang["new_tonggiatritstc"] = new Money(tonggiatritstc - giatritstc);
                            service.Update(khachhang);
                        }
                    }
                }
            }
        }
    }
}
