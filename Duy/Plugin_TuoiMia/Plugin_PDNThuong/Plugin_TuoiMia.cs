using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PDNThuong
{
    public class Plugin_TuoiMia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);

                Entity target = (Entity)context.InputParameters["Target"];
                if (context.MessageName.ToLower().Trim() != "update")
                {
                    return; 
                }
                Entity tuoimia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_thuacanhtac", "new_luongnuoctuoi", "statecode" }));
                Entity chitiethddtm = new Entity();
                var Status = -1;
                if (tuoimia.Contains("statecode"))
                {
                    Status = tuoimia.GetAttributeValue<OptionSetValue>("statecode").Value;
                }
                if (Status == 1)
                {
                    chitiethddtm = service.Retrieve("new_thuadatcanhtac", ((EntityReference)tuoimia["new_thuacanhtac"]).Id, new ColumnSet(new string[] { "new_luongnuoctuoihuuhieu" }));

                    if (!chitiethddtm.Contains("new_luongnuoctuoihuuhieu"))
                    {
                        chitiethddtm["new_luongnuoctuoihuuhieu"] = new decimal(0);
                    }
                    chitiethddtm["new_luongnuoctuoihuuhieu"] = chitiethddtm.GetAttributeValue<decimal>("new_luongnuoctuoihuuhieu") + tuoimia.GetAttributeValue<int>("new_luongnuoctuoi");
                    //throw new Exception(chitiethddtm.GetAttributeValue<decimal>("new_luongnuoctuoihuuhieu").ToString());
                    service.Update(chitiethddtm);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
        }
    }
}
