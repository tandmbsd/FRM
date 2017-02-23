using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;
namespace Complete_DanhGiaNangSuat
{
    public class Complete_DanhGiaNangSuat : IPlugin
    {
        //moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            traceService.Trace("1");
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statecode") && ((OptionSetValue)target["statecode"]).Value == 1)
            {
                traceService.Trace("2");
                Entity danhgianangsuat = service.Retrieve(target.LogicalName, target.Id,
                    new ColumnSet(new string[] { "new_nangsuatdukien", "new_sanluongdukien", "new_chitiethddtmia", "new_sanluonghom" }));
                traceService.Trace("3");

                if (!danhgianangsuat.Contains("new_chitiethddtmia"))
                    throw new Exception("Đánh giá ước năng suất không có chi tiết mía");

                Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac",
                    ((EntityReference)danhgianangsuat["new_chitiethddtmia"]).Id,
                    new ColumnSet(new string[] { "new_sanluonguoc", "new_nangsuatuoc", "new_sanluonguochomgiong" }));

                decimal sanluonguoc = 0;
                decimal nangsuatuoc = 0;
                decimal sanluonghom = 0;

                traceService.Trace("4");
                sanluonguoc = danhgianangsuat.Contains("new_sanluongdukien") ? (decimal)danhgianangsuat["new_sanluongdukien"] : 0;
                nangsuatuoc = danhgianangsuat.Contains("new_nangsuatdukien") ? (decimal)danhgianangsuat["new_nangsuatdukien"] : 0;
                sanluonghom = danhgianangsuat.Contains("new_sanluonghom") ? (decimal)danhgianangsuat["new_sanluonghom"] : 0;

                thuadatcanhtac["new_sanluonguoc"] = sanluonguoc;
                traceService.Trace("5");
                thuadatcanhtac["new_nangsuatuoc"] = nangsuatuoc;
                thuadatcanhtac["new_sanluonguochomgiong"] = sanluonghom;
                traceService.Trace("6");
                service.Update(thuadatcanhtac);
            }
        }
    }
}
