using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_ChitietHDDTTTB_ReloadTLTHVDK
{
    public class Plugin_ChitietHDDTTTB_ReloadTLTHVDK : IPlugin
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
            Entity ChiTietHDDTTrangThietBi = service.Retrieve("new_hopdongdaututrangthietbichitiet", target.Id, new ColumnSet(new string[] { "new_maymocthietbi", "createdon", "new_name", "new_giatrihopdong", "new_hopdongdaututrangthietbi", "new_giatrithietbi", "new_chin" }));

            //throw new Exception("chay plugin tim CSDT 1");

            if (target.Contains("new_giatrihopdong"))
            {
                if (ChiTietHDDTTrangThietBi.Contains("new_chin"))
                {
                    //throw new Exception("chay plugin tim CSDT 2");
                    EntityCollection dsTLTHVDK = FindTLTHVDK(service, ChiTietHDDTTrangThietBi);
                    traceService.Trace("so lung  " + dsTLTHVDK.Entities.Count());

                    if (dsTLTHVDK != null && dsTLTHVDK.Entities.Count() > 0)
                    {
                        foreach (Entity TLTHVDK in dsTLTHVDK.Entities)
                        {
                            Entity en = new Entity(TLTHVDK.LogicalName);
                            en.Id = TLTHVDK.Id;

                            decimal tyle = (TLTHVDK.Contains("new_tylephantram") ? TLTHVDK.GetAttributeValue<decimal>("new_tylephantram") : 0);
                            traceService.Trace("ty le % " + tyle);
                            decimal giatrihopdong = (ChiTietHDDTTrangThietBi.Contains("new_giatrihopdong") ? ChiTietHDDTTrangThietBi.GetAttributeValue<Money>("new_giatrihopdong").Value : ChiTietHDDTTrangThietBi.GetAttributeValue<Money>("new_giatritoida").Value);
                            traceService.Trace("gia tri HD " + giatrihopdong);
                            decimal sotien = (giatrihopdong * tyle) / 100;
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

        public static EntityCollection FindTLTHVDK(IOrganizationService crmservices, Entity chitietHDttb)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_tylethuhoivondukien'>
                        <attribute name='new_name' />
                        <attribute name='new_vudautu' />
                        <attribute name='new_tylephantram' />
                        <attribute name='new_sotienthuhoi' />
                        <attribute name='new_loaityle' />
                        <attribute name='new_chitiethddttrangthietbi' />
                        <attribute name='new_tylethuhoivondukienid' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_chitiethddttrangthietbi' operator='eq' uitype='new_hopdongdaututrangthietbichitiet' value='{0}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, chitietHDttb.Id);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }

    }
}
