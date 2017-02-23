using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Update_SoLanNghiemThu_NTTrongMia
{
    public class Update_SoLanNghiemThu_NTTrongMia : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);

            Entity target = (Entity)context.InputParameters["Target"];

            if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000000)
            {
                Entity NTTrongMia = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(new string[] { "new_lannghiemthu_global", "new_hopdongtrongmia" }));
                int lannghiemthu = ((OptionSetValue)NTTrongMia["new_lannghiemthu_global"]).Value;
                List<Entity> lstChitietNT = RetrieveMultiRecord(service, "new_chitietnghiemthutrongmia", new ColumnSet(new string[] { "new_chitietnghiemthutrongmiaid", "new_thuadat" }), "new_nghiemthutrongmia", NTTrongMia.Id);
                Entity hopdongtrongmia = service.Retrieve("new_hopdongdautumia", ((EntityReference)NTTrongMia["new_hopdongtrongmia"]).Id, new ColumnSet(new string[] { "new_hopdongdautumiaid" }));

                foreach (Entity en in lstChitietNT)
                {
                    if (en.Contains("new_thuadat"))
                    {
                        QueryExpression q = new QueryExpression("new_thuadatcanhtac");
                        q.ColumnSet = new ColumnSet(new string[] { "new_trangthainghiemthu", "statuscode", "new_name", "new_hopdongdautumia" });
                        q.Criteria = new FilterExpression();
                        q.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal,((EntityReference)en["new_thuadat"]).Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal, hopdongtrongmia.Id));
                        EntityCollection entc = service.RetrieveMultiple(q);

                        Entity CTHDDTM = entc.Entities.ToList<Entity>().FirstOrDefault();
                        
                        
                        Entity newCTHDDTM = new Entity(CTHDDTM.LogicalName);
                        newCTHDDTM.Id = CTHDDTM.Id;

                        newCTHDDTM["new_trangthainghiemthu"] = new OptionSetValue(lannghiemthu+2);

                        if (lannghiemthu == 100000005)
                        {
                            newCTHDDTM["statuscode"] = new OptionSetValue(100000002); // nghiem thu xong
                        }
                        else if (lannghiemthu >= 100000000 && lannghiemthu <= 100000002)
                        {
                            newCTHDDTM["statuscode"] = new OptionSetValue(100000001); // nghiem thu xong
                        }

                        service.Update(newCTHDDTM);
                    }
                    break;
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
