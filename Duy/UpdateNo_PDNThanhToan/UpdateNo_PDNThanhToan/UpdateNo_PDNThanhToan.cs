using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace UpdateNo_PDNThanhToan
{
    public class UpdateNo_PDNThanhToan : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000 && context.Depth < 2)
            {
                Entity PDNTT = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                Entity temp = null;

                if (PDNTT.Contains("new_khachhang"))
                    temp = service.Retrieve("contact", ((EntityReference)PDNTT["new_khachhang"]).Id,
                        new ColumnSet(new string[] { "new_no" }));
                else if (PDNTT.Contains("new_khachhangdoanhnghiep"))
                    temp = service.Retrieve("account", ((EntityReference)PDNTT["new_khachhangdoanhnghiep"]).Id,
                        new ColumnSet(new string[] { "new_no" }));

                if (!PDNTT.Contains("new_phieudenghithuno"))
                    return;

                Entity PDNTN = service.Retrieve("new_phieudenghithuno", ((EntityReference)PDNTT["new_phieudenghithuno"]).Id, new ColumnSet(true));

                if (((OptionSetValue)PDNTT["new_loaithanhtoan"]).Value == 100000003) // hom giong mua cua nong dan
                {
                    PDNTN["statuscode"] = new OptionSetValue(100000000);
                    service.Update(PDNTN);
                }
                else
                {
                    if (((OptionSetValue)PDNTN["statuscode"]).Value == 100000000)  // da duyet
                    {
                        PDNTT["new_khautruno"] = new Money(PDNTN.Contains("new_tongtienthu") ? ((Money)PDNTN["new_tongtienthu"]).Value : 0);
                        decimal tongtienthanhtoan = PDNTT.Contains("new_tongtienthanhtoan") ? ((Money)PDNTT["new_tongtienthanhtoan"]).Value : 0;
                        decimal khautruno = PDNTT.Contains("new_khautruno") ? ((Money)PDNTT["new_khautruno"]).Value : 0;
                        Entity t = service.Retrieve(temp.LogicalName, temp.Id, new ColumnSet(new string[] { "new_no" }));

                        if (tongtienthanhtoan >= khautruno)
                        {
                            PDNTT["new_sotienconlai"] = new Money(((Money)PDNTT["new_tongtienthanhtoan"]).Value - ((Money)PDNTT["new_khautruno"]).Value);
                            t["new_no"] = new Money(0);
                        }
                        else
                        {
                            PDNTT["new_sotienconlai"] = new Money(0);
                            t["new_no"] = new Money(((Money)PDNTT["new_khautruno"]).Value - ((Money)PDNTT["new_tongtienthanhtoan"]).Value);
                            PDNTT["new_khautruno"] = new Money(tongtienthanhtoan);
                        }

                        service.Update(PDNTT);
                        service.Update(t);
                    }
                }
            }
        }
    }
}
