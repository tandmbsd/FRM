using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_PDN_GiaiNgan
{
    public class Execution : IPlugin
    {
        private IOrganizationService service;
        private IOrganizationServiceFactory factory;
        private ITracingService trace;
        Entity target = null;
        Entity fullEntity = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

            if (context.MessageName == "Create")
            {
                target = context.InputParameters["Target"] as Entity;
                if (target.LogicalName == "new_chitietphieudenghigiaingan")
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    
                    //if (!target.Contains("new_noidunggiaingan"))
                    //    throw new Exception("Vui lòng điền thông tin nội dung giải ngân !");
                    EntityReference pdkRef = target.ToEntityReference();

                    TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                    tinhDm.CalculateTrongMia(target);
                }
            }
            else if (context.MessageName == "Update" && context.Depth < 2)
            {
                trace.Trace("Start update");
                target = context.InputParameters["Target"] as Entity;
                fullEntity = context.PostEntityImages["PostImg"] as Entity;

                if (target.LogicalName == "new_chitietphieudenghigiaingan")
                {
                    //int statuscode = ((OptionSetValue)fullEntity["new_noidunggiaingan"]).Value;
                    //if (statuscode != 100000000)// đầu tư mía
                    //{
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
                    EntityReference pdkRef = target.ToEntityReference();

                    TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                    tinhDm.CalculateTrongMia(fullEntity);
                    //}
                }
            }
        }
    }
}
