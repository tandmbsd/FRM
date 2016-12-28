using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Text;

namespace Plugin_PDK_DichVu
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
            if (context.MessageName == "Create")
            {                
                target = context.InputParameters["Target"] as Entity;
                if (target.LogicalName == "new_chitietdangkydichvu" && target.Contains("new_phieudangkydichvu"))
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

                    EntityReference pdkRef = (EntityReference)target["new_phieudangkydichvu"];
                    
                    TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                    tinhDm.CalculateTrongMia();

                }
            }
            else if (context.MessageName == "Update")
            {
                target = context.InputParameters["Target"] as Entity;
                fullEntity = context.PostEntityImages["PostImg"] as Entity;
                if (target.LogicalName == "new_chitietdangkydichvu" && fullEntity.Contains("new_phieudangkydichvu") && fullEntity["new_phieudangkydichvu"] != null)
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

                    EntityReference pdkRef = (EntityReference)fullEntity["new_phieudangkydichvu"];

                    TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                    tinhDm.CalculateTrongMia();

                }
                else if (target.Contains("new_tinhtrangduyet"))
                {
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value != 100000002)
                    {
                        factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                        service = factory.CreateOrganizationService(context.UserId);
                        trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
                        EntityReference pdkRef = target.ToEntityReference();

                        TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                        tinhDm.CalculateTrongMia();

                    }
                    else
                    {
                        factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                        service = factory.CreateOrganizationService(context.UserId);
                        trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

                        EntityReference pdkRef = target.ToEntityReference();
                        decimal tongtien = sum_current_ct_pdk(pdkRef);

                        decimal tokhl = (fullEntity.Contains("new_denghi_khonghoanlai") ? ((Money)fullEntity["new_denghi_khonghoanlai"]).Value : 0);
                        decimal tohl = (fullEntity.Contains("new_denghi_hoanlai_tienmat") ? ((Money)fullEntity["new_denghi_hoanlai_tienmat"]).Value : 0) + (fullEntity.Contains("new_denghi_hoanlai_vattu") ? ((Money)fullEntity["new_denghi_hoanlai_vattu"]).Value : 0);
                        if (tongtien != (tohl + tokhl))
                            throw new Exception("Số tiền chi tiết đăng ký khác số tiền đề nghị chi, vui lòng kiểm tra lại !");
                    }
                }
            }
            else if (context.MessageName == "Delete")
            {
                fullEntity = context.PreEntityImages["PreImg"] as Entity;
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
                EntityReference pdkRef = (EntityReference)fullEntity["new_phieudangkydichvu"];

                TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                tinhDm.CalculateTrongMia();
            }
        }

        private decimal sum_current_ct_pdk(EntityReference pdkRef)
        {
            decimal result = 0;
            StringBuilder fetch = new StringBuilder();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_chitietdangkydichvu'>");
            fetch.AppendFormat("<attribute name='new_thanhtien' alias='thanhtien' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_phieudangkydichvu' operator='eq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("thanhtien"))
                {
                    AliasedValue als = (AliasedValue)tmp["thanhtien"];
                    if (als.Value != null)
                        result = ((Money)((AliasedValue)tmp["thanhtien"]).Value).Value;
                }
            }
            return result;
        }
    }
}
