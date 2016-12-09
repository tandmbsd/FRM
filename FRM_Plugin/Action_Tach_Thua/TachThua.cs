using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Action_Tach_Thua
{
    public class TachThua : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            EntityReference target = (EntityReference)context.InputParameters["Target"];
            if (target.LogicalName == "new_thuadat")
            {
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                service = factory.CreateOrganizationService(context.UserId);
                Entity thuadat = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                if (thuadat == null)
                    throw new Exception("Thửa đất này không tồn tại!");
                Entity new_thua = new Entity(thuadat.LogicalName);
                foreach (string key in thuadat.Attributes.Keys)
                {
                    if (key.IndexOf("new_") == 0 && key != thuadat.LogicalName + "id")
                        new_thua[key] = thuadat[key];
                }
                new_thua["new_thuachinh"] = target;
                new_thua["new_name"] = string.Format("Thửa tách từ - {0}", new_thua["new_name"]);
                Guid new_thuaid = service.Create(new_thua);

                QueryExpression q = new QueryExpression("new_thuyloi");
                q.ColumnSet = new ColumnSet(true);
                q.Criteria = new FilterExpression();
                q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, thuadat.Id));
                EntityCollection entc = service.RetrieveMultiple(q);
                EntityReference new_thuadatRef = new EntityReference(new_thua.LogicalName, new_thuaid);
                foreach (Entity en in entc.Entities)
                {
                    Entity tmp = new Entity(en.LogicalName);
                    foreach (string key in en.Attributes.Keys)
                    {
                        if (key.IndexOf("new_") == 0 && key != en.LogicalName + "id")
                            tmp[key] = en[key];
                    }
                    tmp["new_thuadat"] = new_thuadatRef;
                    service.Create(tmp);
                }


                //q = new QueryExpression("new_new_thuadatkh_chusohuu");
                //q.ColumnSet = new ColumnSet(new string[]{"new_thuadatid","contactid"});
                //q.Criteria = new FilterExpression();
                //q.Criteria.AddCondition("new_thuadatid", ConditionOperator.Equal, thuadat.Id);
                //entc = service.RetrieveMultiple(q);
                //if(entc.Entities.Count > 0)
                //{
                //    EntityReferenceCollection entcRef_thuadat_contact = new EntityReferenceCollection();
                //    foreach (Entity en in entc.Entities)
                //        entcRef_thuadat_contact.Add((new EntityReference("contact", (Guid)en["contactid"])));
                //    service.Associate(new_thua.LogicalName, new_thuaid, new Relationship("new_new_thuadatkh_chusohuu"), entcRef_thuadat_contact);
                //}

                context.OutputParameters["ReturnId"] = new_thuaid.ToString();
            }
        }
    }
}
