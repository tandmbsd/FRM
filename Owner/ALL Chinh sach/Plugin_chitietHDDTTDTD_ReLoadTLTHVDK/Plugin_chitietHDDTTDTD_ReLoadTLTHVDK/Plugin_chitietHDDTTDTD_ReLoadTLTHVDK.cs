using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_chitietHDDTTDTD_ReLoadTLTHVDK
{
    public class Plugin_chitietHDDTTDTD_ReLoadTLTHVDK : IPlugin
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
            Entity ChiTietHDThueDatThuaDat = service.Retrieve("new_chitiethdthuedat_thuadat", target.Id, new ColumnSet(new string[] { "new_sotiendautu", "new_sotiendaututhucte", "new_sotienthuethucthue", "new_chinhsachdautu" }));

            //throw new Exception("chay plugin tim CSDT 1");

            if (target.Contains("new_sotiendautu") || target.Contains("new_sotiendaututhucte"))
            {
                //throw new Exception("chay plugin tim CSDT 1");
                if (ChiTietHDThueDatThuaDat.Contains("new_chinhsachdautu"))
                {
                    EntityCollection dsTLTHVDK = FindTLTHVDK(service, ChiTietHDThueDatThuaDat);
                    //traceService.Trace("so luong  " + dsTLTHVDK.Entities.Count());

                    if (dsTLTHVDK != null && dsTLTHVDK.Entities.Count() > 0)
                    {
                        foreach (Entity TLTHVDK in dsTLTHVDK.Entities)
                        {
                            Entity en = new Entity(TLTHVDK.LogicalName);
                            en.Id = TLTHVDK.Id;

                            decimal tyle = (TLTHVDK.Contains("new_tylephantram") ? TLTHVDK.GetAttributeValue<decimal>("new_tylephantram") : 0);
                            traceService.Trace("ty le % " + tyle);

                            decimal sotien = 0;
                            decimal sotiendautu = 0;
                            //if (ChiTietHDThueDatThuaDat.Contains("new_sotienthuethucthue"))
                            //{
                            //    sotiendautu = (ChiTietHDThueDatThuaDat.Contains("new_sotienthuethucthue") ? ChiTietHDThueDatThuaDat.GetAttributeValue<Money>("new_sotienthuethucthue").Value : 0);
                            //    traceService.Trace("gia tri HD " + sotiendautu);
                            //}
                            if (ChiTietHDThueDatThuaDat.Contains("new_sotiendaututhucte"))
                            {
                                sotiendautu = (ChiTietHDThueDatThuaDat.Contains("new_sotiendaututhucte") ? ChiTietHDThueDatThuaDat.GetAttributeValue<Money>("new_sotiendaututhucte").Value : 0);
                                traceService.Trace("gia tri HD " + sotiendautu);
                            }
                            else if(ChiTietHDThueDatThuaDat.Contains("new_sotiendautu"))
                            {
                                sotiendautu = (ChiTietHDThueDatThuaDat.Contains("new_sotiendautu") ? ChiTietHDThueDatThuaDat.GetAttributeValue<Money>("new_sotiendautu").Value : 0);
                                traceService.Trace("gia tri HD " + sotiendautu);
                            }
                            
                            sotien = (sotiendautu * tyle) / 100;
                            traceService.Trace("so tien " + sotien);

                            en["new_sotienthuhoi"] = new Money(sotien);

                            service.Update(en);
                            traceService.Trace("update xong ");
                        }
                    } // if (dsTLTHVDK != null && dsTLTHVDK.Entities.Count() > 0)
                    //throw new Exception("chay plugin tim CSDT 3");
                }
            }
        }

        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHDDTTDthuadat)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_tylehopdong' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethdthuedat_thuadat' operator='eq' uitype='new_chitiethdthuedat_thuadat' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHDDTTDthuadat.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
    }
}

