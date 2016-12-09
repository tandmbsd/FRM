using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_BienBanGiamHuy
{
    public class Plugin_BienBanGiamHuy : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            Entity BBGiamHuy = service.Retrieve("new_bienbangiamhuydientich", target.Id, new ColumnSet(true));

            if (target.Contains("new_trangthai"))
            {
                if (((OptionSetValue)target["new_trangthai"]).Value == 100000002)
                {
                    Entity chitietHD = service.Retrieve("new_thuadatcanhtac", ((EntityReference)target["new_thuadatcanhtac"]).Id, new ColumnSet(new string[] { "new_thuadatcanhtacid", "new_dientichgiamhuy", "new_dientichconlai" }));
                    if (((OptionSetValue)BBGiamHuy["new_loai"]).Value == 100000000) // Giảm
                    {
                        chitietHD["new_dientichgiamhuy"] = (chitietHD.Contains("new_dientichgiamhuy") ? (decimal)chitietHD["new_dientichgiamhuy"] : 0) + (decimal)BBGiamHuy["new_dientichgiamhuy"];
                        chitietHD["new_dientichconlai"] = (chitietHD.Contains("new_dientichthucte") ? (decimal)chitietHD["new_dientichthucte"] : 0) - (decimal)chitietHD["new_dientichgiamhuy"];
                        
                        service.Update(chitietHD);
                    }
                    else //Hủy
                    {
                        chitietHD["new_lydohuy"] = ((EntityReference)BBGiamHuy["new_lydogiamhuy"]);
                        chitietHD["new_ngayhuy"] = BBGiamHuy.Attributes.Contains("new_ngaylap") ? BBGiamHuy["new_ngaylap"] : null;
                        service.Update(chitietHD);

                        EntityReference moniker = new EntityReference();
                        moniker.LogicalName = "new_thuadatcanhtac";
                        moniker.Id = chitietHD.Id;

                        OrganizationRequest request = new Microsoft.Xrm.Sdk.OrganizationRequest() { RequestName = "SetState" };
                        request["EntityMoniker"] = moniker;
                        OptionSetValue state = new OptionSetValue(1);
                        OptionSetValue status = new OptionSetValue(100000005);
                        request["State"] = state;
                        request["Status"] = status;

                        service.Execute(request);
                    }
                }
            }
        }
    }
}
