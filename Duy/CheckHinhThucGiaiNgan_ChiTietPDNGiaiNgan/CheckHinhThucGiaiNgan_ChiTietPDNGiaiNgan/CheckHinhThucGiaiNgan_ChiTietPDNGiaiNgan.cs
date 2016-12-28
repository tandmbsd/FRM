using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;


namespace CheckHinhThucGiaiNgan_ChiTietPDNGiaiNgan
{
    public class CheckHinhThucGiaiNgan_ChiTietPDNGiaiNgan : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            if (context.MessageName.ToLower().Trim() == "create" || context.MessageName.ToLower().Trim() == "update")
            {
                Entity target = (Entity)context.InputParameters["Target"];
                if (target.Contains("new_loaigiaingan") && ((OptionSetValue)target["new_loaigiaingan"]).Value == 100000000)
                {
                    
                }
            }
        }
    }
}
