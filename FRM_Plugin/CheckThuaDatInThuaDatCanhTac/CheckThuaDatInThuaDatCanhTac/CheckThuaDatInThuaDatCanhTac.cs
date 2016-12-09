using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace CheckThuaDatInThuaDatCanhTac
{
    public class CheckThuaDatInThuaDatCanhTac : IPlugin
    {
        // moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;


        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            trace.Trace("12321");
            Entity target = (Entity)context.InputParameters["Target"];
            trace.Trace("1");
            if (target.Contains("new_hopdongdautumia") && context.Depth == 1)
            {
                Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)target["new_hopdongdautumia"]).Id, new ColumnSet
                    (new string[] { "new_hopdongdautumiaid", "statuscode" }));

                if (hddtm.Contains("statuscode") && ((OptionSetValue)hddtm["statuscode"]).Value == 100000003)
                {
                    throw new Exception("Hợp đồng đã ký không được phép thêm chi tiết hợp đồng đầu tư mía !!!");
                }
            }
            trace.Trace("2");

            if (target.Contains("new_thuadat") && target.Contains("new_hopdongdautumia"))
            {
                trace.Trace("3");
                Entity thuadat = service.Retrieve("new_thuadat", ((EntityReference)target["new_thuadat"]).Id, new ColumnSet(new string[] { "new_diachi" }));
                trace.Trace("4");
                //if(!thuadatcanhtac.Contains("new_hopdongdautumia")){
                //    throw new Exception("Chưa chọn hợp đồng tư mía !! ");
                //}
                Entity hopdongdatumia = service.Retrieve("new_hopdongdautumia", ((EntityReference)target["new_hopdongdautumia"]).Id, new ColumnSet(true));
                if (hopdongdatumia.Contains("new_quocgia") && thuadat.Contains("new_diachi"))
                {
                    Entity diachi = service.Retrieve("new_diachi", ((EntityReference)thuadat["new_diachi"]).Id, new ColumnSet(new string[] { "new_quocgia" }));

                    if (((EntityReference)hopdongdatumia["new_quocgia"]).Id != ((EntityReference)diachi["new_quocgia"]).Id)
                    {
                        throw new Exception("Quốc gia của thửa đất và hợp đồng tư mía không giống nhau !! ");
                    }
                }
            }
        }
    }
}
