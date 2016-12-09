using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_AutogenTuoiMiaInChitietNTTuoi
{
    public class Plugin_AutogenTuoiMiaInChitietNTTuoi : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            Entity target = (Entity)context.InputParameters["Target"];
            Entity CTNTTuoiMia = service.Retrieve("new_chitietnghiemthutuoimia", target.Id, new ColumnSet(new string[] { "new_nghiemthutuoimia" }));
            Entity ChitietHDMia;

            if (target.Contains("new_thuadat") && CTNTTuoiMia.Contains("new_nghiemthutuoimia"))
            {
                //del all new_chitietnghiemthutuoimia_tuoimia cũ
                QueryExpression qexoa = new QueryExpression("new_chitietnghiemthutuoimia_tuoimia");
                qexoa.ColumnSet = new ColumnSet(new string[] { "new_chitietnghiemthutuoimia_tuoimiaid" });
                qexoa.Criteria.AddCondition(new ConditionExpression("new_chitietnghiemthutuoimia", ConditionOperator.Equal, target.Id));
                foreach (Entity a in service.RetrieveMultiple(qexoa).Entities)
                    service.Delete(a.LogicalName, a.Id);

                //get Chi tiết hợp đồng mía
                QueryExpression qe = new QueryExpression("new_thuadatcanhtac");
                qe.ColumnSet = new ColumnSet(new string[] { "new_thuadatcanhtacid" });

                var linktoHDdautumia = new LinkEntity("new_thuadatcanhtac", "new_hopdongdautumia", "new_hopdongdautumia", "new_hopdongdautumiaid", JoinOperator.Inner);
                var linktoNTTuoimia = new LinkEntity("new_hopdongdautumia", "new_nghiemthutuoimia", "new_hopdongdautumiaid", "new_hopdongtrongmia", JoinOperator.Inner);
                linktoNTTuoimia.LinkCriteria.AddCondition(new ConditionExpression("activityid", ConditionOperator.Equal, ((EntityReference)CTNTTuoiMia["new_nghiemthutuoimia"]).Id));
                linktoHDdautumia.LinkEntities.Add(linktoNTTuoimia);
                qe.LinkEntities.Add(linktoHDdautumia);
                qe.Criteria.AddCondition(new ConditionExpression("new_thuadat", ConditionOperator.Equal, ((EntityReference)target["new_thuadat"]).Id));

                EntityCollection result = service.RetrieveMultiple(qe);
                if (result.Entities.Count > 0)
                {
                    ChitietHDMia = result[0];

                    //get all active tưới mía and create.
                    QueryExpression allTuoi = new QueryExpression("new_tuoimia");
                    allTuoi.ColumnSet = new ColumnSet(new string[] { "activityid", "subject" });
                    allTuoi.Criteria.AddCondition(new ConditionExpression("new_thuacanhtac", ConditionOperator.Equal, result[0].Id));
                    allTuoi.Criteria.AddCondition(new ConditionExpression("statecode", ConditionOperator.Equal, 1));

                    EntityCollection dsTuoi = service.RetrieveMultiple(allTuoi);
                    if (dsTuoi.Entities.Count > 0)
                        foreach (Entity a in dsTuoi.Entities)
                        {
                            Entity l = new Entity("new_chitietnghiemthutuoimia_tuoimia");
                            l["new_chitietnghiemthutuoimia"] = new EntityReference("new_chitietnghiemthutuoimia", target.Id);
                            l["new_tuoimia"] = new EntityReference("new_tuoimia", a.Id);
                            l["new_name"] = a.Contains("subject") ? a["subject"].ToString() : "No name";
                            service.Create(l);
                        }

                    //set data to update Chi tiet nghiem thu tuoi mia

                    Entity upCTNTTuoi = new Entity("new_nghiemthutuoimia");
                    upCTNTTuoi.Id = target.Id;
                    upCTNTTuoi["new_solantuoi"] = result.Entities.Count;
                    upCTNTTuoi["new_mucdichtuoi"] = new OptionSetValue(result.Entities.Count <= 2 ? 100000000 : 100000001);

                    //Get chính sách and set data CTNT Tưới mía 
                    //................
                    #region begin set chính sách
                    #endregion

                    service.Update(upCTNTTuoi);

                }
            }
        }
    }
}
