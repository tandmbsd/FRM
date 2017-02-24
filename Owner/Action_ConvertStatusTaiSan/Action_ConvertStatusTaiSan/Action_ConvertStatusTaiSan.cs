using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace Action_ConvertStatusTaiSan
{
    public class Action_ConvertStatusTaiSan : IPlugin
    {
        private IOrganizationServiceFactory factory;
        private IOrganizationService service;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            var target = (EntityReference)context.InputParameters["Target"];

            if (target.LogicalName == "new_taisanthechap")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                var Updtaisanthechap = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "statuscode" }));

                if (Updtaisanthechap == null)
                    throw new Exception("Tài sản thế chấp này không tồn tại !!");

                Updtaisanthechap["statuscode"] = new OptionSetValue(100000000);
                service.Update(Updtaisanthechap);

                var taisanthechap = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_taisan" }));
                Guid tstcId = taisanthechap.Id;

                if (taisanthechap.Contains("new_taisan"))
                {
                    Entity taisan = service.Retrieve("new_taisan", ((EntityReference)taisanthechap["new_taisan"]).Id,
                        new ColumnSet(new string[] { "new_trangthaitaisan" }));

                    taisan["new_trangthaitaisan"] = new OptionSetValue(100000002);
                    service.Update(taisan);
                }
                
                context.OutputParameters["ReturnId"] = tstcId.ToString();
            }
        }
    }
}
