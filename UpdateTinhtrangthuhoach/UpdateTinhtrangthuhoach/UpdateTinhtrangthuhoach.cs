using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization;

namespace UpdateTinhtrangthuhoach
{
    public class UpdateTinhtrangthuhoach : IPlugin
    {
        private IOrganizationService service = null;
        private IOrganizationServiceFactory factory = null;
        public ITracingService trace;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000) // da duyet
            {
                Entity lenhdon = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_thuacanhtac", "new_lenhdoncuoi","new_loaihopdong" }));

                if (lenhdon.Contains("new_loaihopdong") && ((OptionSetValue)lenhdon["new_loaihopdong"]).Value == 100000000)
                {
                    if (!lenhdon.Contains("new_thuacanhtac"))
                        throw new Exception("Lệnh đốn không có chi tiết HĐ mía");

                    Entity chitiethdmia = service.Retrieve("new_thuadatcanhtac", ((EntityReference)lenhdon["new_thuacanhtac"]).Id,
                        new ColumnSet(new string[] { "new_tinhtrangthuhoach" }));

                    if (lenhdon.Contains("new_lenhdoncuoi") && (bool)lenhdon["new_lenhdoncuoi"] == true)
                        chitiethdmia["new_tinhtrangthuhoach"] = new OptionSetValue(100000002);
                    else
                        chitiethdmia["new_tinhtrangthuhoach"] = new OptionSetValue(100000001);

                    service.Update(chitiethdmia);
                }
            }
        }
    }
}
