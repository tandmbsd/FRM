using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_TinhtienTSTC
{
    public class Plugin_TinhtienTSTC : IPlugin
    {
        private IOrganizationServiceFactory factory;
        private IOrganizationService service;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext) serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory) serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            var traceService = (ITracingService) serviceProvider.GetService(typeof(ITracingService));
            var target = (Entity) context.InputParameters["Target"];

            if (target.Contains("statuscode"))
            {
                var taisanthechap = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet("new_hopdongthechap", "new_giatridinhgiagiatrithechap"));
                var giatritstc = taisanthechap.Contains("new_giatridinhgiagiatrithechap")
                    ? ((Money) taisanthechap["new_giatridinhgiagiatrithechap"]).Value
                    : new decimal(0);

                var hopdongthechap = service.Retrieve("new_hopdongthechap",
                    ((EntityReference) taisanthechap["new_hopdongthechap"]).Id,
                    new ColumnSet("statuscode", "new_benthuba", "new_nguoidambaochinhkhcn",
                        "new_nguoiduocdambaochinhkhdn", "new_chuhopdong", "new_chuhopdongdoanhnghiep"));

                var hopdong3ben = hopdongthechap.Contains("new_benthuba")
                    ? (bool) hopdongthechap["new_benthuba"]
                    : false;
                traceService.Trace(hopdong3ben.ToString());
                if (((OptionSetValue) hopdongthechap["statuscode"]).Value == 100000000) // da ky
                {
                    Entity khachhang = null;
                    decimal tonggiatritstc = 0;

                    if (((OptionSetValue) target["statuscode"]).Value == 100000001) // giai chap
                    {
                        traceService.Trace("chay if giai chap");
                        if (hopdong3ben == false)
                        {
                            traceService.Trace("1");
                            if (hopdongthechap.Contains("new_chuhopdong"))
                                khachhang = service.Retrieve("contact",
                                    ((EntityReference) hopdongthechap["new_chuhopdong"]).Id,
                                    new ColumnSet("new_tonggiatritstc", "fullname"));
                            else if (hopdongthechap.Contains("new_chuhopdongdoanhnghiep"))
                                khachhang = service.Retrieve("account",
                                    ((EntityReference) hopdongthechap["new_chuhopdongdoanhnghiep"]).Id,
                                    new ColumnSet("new_tonggiatritstc"));
                        }
                        else
                        {
                            traceService.Trace("2");
                            if (hopdongthechap.Contains("new_nguoidambaochinhkhcn"))
                            {
                                khachhang = service.Retrieve("contact",
                                    ((EntityReference) hopdongthechap["new_nguoidambaochinhkhcn"]).Id,
                                    new ColumnSet("new_tonggiatritstc", "fullname"));

                                traceService.Trace("3");
                            }
                            else if (hopdongthechap.Contains("new_nguoiduocdambaochinhkhdn"))
                            {
                                khachhang = service.Retrieve("account",
                                    ((EntityReference) hopdongthechap["new_nguoiduocdambaochinhkhdn"]).Id,
                                    new ColumnSet("new_tonggiatritstc"));

                                traceService.Trace("4");
                            }
                        }

                        tonggiatritstc = khachhang.Contains("new_tonggiatritstc")
                            ? ((Money) khachhang["new_tonggiatritstc"]).Value
                            : new decimal(0);

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