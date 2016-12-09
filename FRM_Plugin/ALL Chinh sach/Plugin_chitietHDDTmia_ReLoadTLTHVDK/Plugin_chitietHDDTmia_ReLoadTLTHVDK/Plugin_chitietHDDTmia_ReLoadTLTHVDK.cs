using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_chitietHDDTmia_ReLoadTLTHVDK
{
    public class Plugin_chitietHDDTmia_ReLoadTLTHVDK : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            //throw new Exception("chay plugin tim CSDT 1");

            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            Entity ChiTietHD = service.Retrieve("new_thuadatcanhtac", target.Id, new ColumnSet(new string[] { "new_chinhsachdautu", "new_dautuhoanlai", "new_tongchihoanlai_hientai", "new_conlai_hoanlai" }));

            //throw new Exception("chay plugin tim CSDT 1");

            if (target.Contains("new_dautuhoanlai") || target.Contains("new_tongchihoanlai_hientai") || target.Contains("new_conlai_hoanlai"))
            {
                if (ChiTietHD.Contains("new_chinhsachdautu"))
                {
                    EntityCollection dsTLTHVDK = FindTLTHVDK(service, ChiTietHD);
                    traceService.Trace("so luong  " + dsTLTHVDK.Entities.Count());

                    if (dsTLTHVDK != null && dsTLTHVDK.Entities.Count() > 0)
                    {
                        foreach (Entity TLTHVDK in dsTLTHVDK.Entities)
                        {
                            Entity en = new Entity(TLTHVDK.LogicalName);
                            en.Id = TLTHVDK.Id;

                            decimal tyle = (TLTHVDK.Contains("new_tylephantram") ? TLTHVDK.GetAttributeValue<decimal>("new_tylephantram") : 0);
                            traceService.Trace("ty le % " + tyle);

                            decimal sotien = 0;

                            if (ChiTietHD.Contains("new_conlai_hoanlai"))
                            {
                                decimal giatrihopdong = (ChiTietHD.Contains("new_conlai_hoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_conlai_hoanlai").Value : 0);
                                traceService.Trace("gia tri HD " + giatrihopdong);

                                sotien = (giatrihopdong * tyle) / 100;
                                traceService.Trace("so tien " + sotien);
                            }
                            else
                            {
                                if (ChiTietHD.Contains("new_tongchihoanlai_hientai"))
                                {
                                    decimal giatrihopdong = (ChiTietHD.Contains("new_tongchihoanlai_hientai") ? ChiTietHD.GetAttributeValue<Money>("new_tongchihoanlai_hientai").Value : 0);
                                    traceService.Trace("gia tri HD " + giatrihopdong);

                                    sotien = (giatrihopdong * tyle) / 100;
                                    traceService.Trace("so tien " + sotien);
                                }
                                else
                                {
                                    if(ChiTietHD.Contains("new_dautuhoanlai"))
                                    {
                                        decimal giatrihopdong = (ChiTietHD.Contains("new_dautuhoanlai") ? ChiTietHD.GetAttributeValue<Money>("new_dautuhoanlai").Value : 0);
                                        traceService.Trace("gia tri HD " + giatrihopdong);

                                        sotien = (giatrihopdong * tyle) / 100;
                                        traceService.Trace("so tien " + sotien);
                                    }
                                }
                            }

                            en["new_sotienthuhoi"] = new Money(sotien);

                            service.Update(en);
                            traceService.Trace("update xong ");
                        }
                    } // if (dsTLTHVDK != null && dsTLTHVDK.Entities.Count() > 0)
                    //throw new Exception("chay plugin tim CSDT 3");
                }
            }
        }

        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHD)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_chitiethddtmia' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddtmia' operator='eq' uitype='new_thuadatcanhtac' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHD.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
    }
}
