using System;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System.Text;

namespace Plugin_PDK_VatTu
{
    public class Execution : IPlugin
    {
        private IOrganizationService service;
        private IOrganizationServiceFactory factory;
        
        Entity fullEntity = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            Entity target = context.InputParameters["Target"] as Entity;

            if (context.MessageName == "Create" && target.LogicalName == "new_phieudangkyvattu")
            {
                trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                EntityReference pdkRef = target.ToEntityReference();

                TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                tinhDm.CalculateTrongMia();
            }

            if (context.MessageName == "Create" && target.LogicalName != "new_phieudangkyvattu")
            {
                trace.Trace("1");
                
                if (target.LogicalName == "new_chitietdangkyvattu" && target.Contains("new_phieudangkyvattu"))
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

                    EntityReference pdkRef = (EntityReference)target["new_phieudangkyvattu"];

                    TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                    tinhDm.CalculateTrongMia();
                }
            }
            else if (context.MessageName == "Update")
            {
                trace.Trace("2");
                
                fullEntity = context.PostEntityImages["PostImg"] as Entity;
                if (target.LogicalName == "new_chitietdangkyvattu")
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

                    EntityReference pdkRef = (EntityReference)fullEntity["new_phieudangkyvattu"];

                    TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                    tinhDm.CalculateTrongMia();
                }
                else if (target.Contains("new_tinhtrangduyet"))
                {
                    if (((OptionSetValue)target["new_tinhtrangduyet"]).Value != 100000002)
                    {
                        trace.Trace("3");
                        factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                        service = factory.CreateOrganizationService(context.UserId);
                        trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
                        EntityReference pdkRef = target.ToEntityReference();

                        TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                        tinhDm.CalculateTrongMia();
                    }
                    else
                    {
                        {
                            trace.Trace("4");
                            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                            service = factory.CreateOrganizationService(context.UserId);
                            trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;

                            EntityReference pdkRef = target.ToEntityReference();
                            decimal hl = 0;
                            decimal khl = 0;
                            sum_current_ct_pdk(pdkRef, ref hl, ref khl);
                            decimal tokhl = (fullEntity.Contains("new_denghi_khonghoanlai") ? ((Money)fullEntity["new_denghi_khonghoanlai"]).Value : 0);
                            decimal tohl = (fullEntity.Contains("new_denghi_hoanlai_tienmat") ? ((Money)fullEntity["new_denghi_hoanlai_tienmat"]).Value : 0) + (fullEntity.Contains("new_denghi_hoanlai_vattu") ? ((Money)fullEntity["new_denghi_hoanlai_vattu"]).Value : 0);
                            if (hl != tohl || khl != tokhl)
                                throw new Exception("Số tiền chi tiết đăng ký khác số tiền đề nghị chi, vui lòng kiểm tra lại !");
                        }
                    }
                }
            }
            else if (context.MessageName == "Delete")
            {
                fullEntity = context.PreEntityImages["PreImg"] as Entity;
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                trace = serviceProvider.GetService(typeof(ITracingService)) as ITracingService;
                EntityReference pdkRef = (EntityReference)fullEntity["new_phieudangkyvattu"];

                TinhDinhMuc tinhDm = new TinhDinhMuc(service, trace, pdkRef);
                tinhDm.CalculateTrongMia();
            }
        }
        
        private void sum_current_ct_pdk(EntityReference pdkRef, ref decimal hl, ref decimal khl)
        {
            decimal tmpHl = 0;
            decimal tmp0hl = 0;
            StringBuilder fetch = new StringBuilder();
            fetch.AppendFormat("<fetch mapping='logical' aggregate='true' version='1.0'>");
            fetch.AppendFormat("<entity name='new_chitietdangkyvattu'>");
            fetch.AppendFormat("<attribute name='new_sotienkhl' alias='khl' aggregate='sum' />");
            fetch.AppendFormat("<attribute name='new_sotienhl' alias='hl' aggregate='sum' />");
            fetch.AppendFormat("<filter type='and'>");
            fetch.AppendFormat("<condition attribute='new_phieudangkyvattu' operator='eq' value='{0}'/>", pdkRef.Id);
            fetch.AppendFormat("</filter>");
            fetch.AppendFormat("</entity>");
            fetch.AppendFormat("</fetch>");
            EntityCollection etnc = service.RetrieveMultiple(new FetchExpression(fetch.ToString()));
            if (etnc.Entities.Count > 0)
            {
                Entity tmp = etnc.Entities[0];
                if (tmp.Contains("hl"))
                {
                    AliasedValue als = (AliasedValue)tmp["hl"];
                    if (als.Value != null)
                        tmpHl = ((Money)((AliasedValue)tmp["hl"]).Value).Value;
                }

                if (tmp.Contains("khl"))
                {
                    AliasedValue als = (AliasedValue)tmp["khl"];
                    if (als.Value != null)
                        tmp0hl = ((Money)((AliasedValue)tmp["khl"]).Value).Value;
                }

            }
            hl = tmpHl;
            khl = tmp0hl;
        }

    }
}
