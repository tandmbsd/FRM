using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace LoadSauHaiBenhHai_NTDauBoSungVon
{
    public class LoadSauHaiBenhHai_NTDauBoSungVon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];
            if (target.Contains("new_danhgiasinhtruong"))
            {
                Entity nghiemthubosungvon = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_danhgiasinhtruong" }));

                List<Entity> lstChitietsauhai = RetrieveMultiRecord(service, "new_chitietsauhai",
                    new ColumnSet(true), "new_danhgiasinhtruong", ((EntityReference)nghiemthubosungvon["new_danhgiasinhtruong"]).Id);

                List<Entity> lstChitietbenhhai = RetrieveMultiRecord(service, "new_chitietbenhhai",
                    new ColumnSet(true), "new_danhgiasinhtruong", ((EntityReference)nghiemthubosungvon["new_danhgiasinhtruong"]).Id);

                foreach (Entity sh in lstChitietsauhai)
                {
                    Entity k = service.Retrieve(sh.LogicalName, sh.Id, new ColumnSet(true));
                    k["new_name"] = sh["new_name"];
                    k["new_sauhai"] = sh["new_sauhai"];
                    k["new_danhgiasinhtruong"] = sh["new_danhgiasinhtruong"];
                    k["new_danhgianangsuat"] = sh["new_danhgianangsuat"];
                    k["new_mucdogayhaioption"] = sh["new_mucdogayhaioption"];

                    service.Create(k);
                }

                foreach (Entity bh in lstChitietbenhhai)
                {
                    Entity k = service.Retrieve(bh.LogicalName, bh.Id, new ColumnSet(true));
                    k["new_name"] = bh["new_name"];
                    k["new_benhhai"] = bh["new_benhhai"];
                    k["new_danhgiasinhtruong"] = bh["new_danhgiasinhtruong"];
                    k["new_danhgianangsuat"] = bh["new_danhgianangsuat"];
                    k["new_mucdogayhai"] = bh["new_mucdogayhai"];

                    service.Create(k);
                }
            }
        }

        List<Entity> RetrieveMultiRecord(IOrganizationService crmservices, string entity, ColumnSet column, string condition, object value)
        {
            QueryExpression q = new QueryExpression(entity);
            q.ColumnSet = column;
            q.Criteria = new FilterExpression();
            q.Criteria.AddCondition(new ConditionExpression(condition, ConditionOperator.Equal, value));
            EntityCollection entc = service.RetrieveMultiple(q);

            return entc.Entities.ToList<Entity>();
        }
    }
}
