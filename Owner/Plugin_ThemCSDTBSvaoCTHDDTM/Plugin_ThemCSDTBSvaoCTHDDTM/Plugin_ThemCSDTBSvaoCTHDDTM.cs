using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Plugin_ThemCSDTBSvaoCTHDDTM
{
    public class Plugin_ThemCSDTBSvaoCTHDDTM : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));

            service = factory.CreateOrganizationService(context.UserId);

            EntityReference targetEntity = null;
            string relationshipName = string.Empty;
            EntityReferenceCollection relatedEntities = null;
            EntityReference relatedEntity = null;

            Entity thuadatcanhtac = new Entity();
            Entity khuyenkhichpt = new Entity();

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                //get the "relationship"
                if (context.InputParameters.Contains("Relationship"))
                    relationshipName = context.InputParameters["Relationship"].ToString();

                //check the relationshipname with intended one
                if (relationshipName != "new_new_chitiethddtmia_new_khuyenkhichpt.")
                    return;

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    thuadatcanhtac = service.Retrieve(targetEntity.LogicalName, targetEntity.Id,
                        new ColumnSet(new string[] { "new_dinhmucdautukhonghoanlai", "createdon",
                            "new_hopdongdautumia","new_dinhmucdautuhoanlai" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;

                    if (relatedEntities.Count > 0)
                    {
                        relatedEntity = relatedEntities[0];
                        khuyenkhichpt = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id,
                            new ColumnSet(new string[] { "new_khuyenkhichphattrienid" }));
                    }
                    else
                        return;

                    DateTime ngaytao = (DateTime)thuadatcanhtac["createdon"];

                    decimal dmkhl = ((Money)thuadatcanhtac["new_dinhmucdautukhonghoanlai"]).Value;
                    decimal dmhl = ((Money)thuadatcanhtac["new_dinhmucdautuhoanlai"]).Value;

                    Entity hddtm = service.Retrieve("new_hopdongdautumia", ((EntityReference)thuadatcanhtac["new_hopdongdautumia"]).Id,
                        new ColumnSet(new string[] { "new_vudautu" }));

                    Entity Vudautu = service.Retrieve("new_vudautu", ((EntityReference)hddtm["new_vudautu"]).Id,
                        new ColumnSet(new string[] { "new_name" }));

                    EntityCollection dsCSDTBSbyKKPT = FindCSDTBSbyKKPT(service, khuyenkhichpt, Vudautu, ngaytao);

                    if (dsCSDTBSbyKKPT != null && dsCSDTBSbyKKPT.Entities.Count > 0)
                    {
                        Entity a = dsCSDTBSbyKKPT[0];

                        decimal tienbskhl = ((Money)a["new_sotienbosung_khl"]).Value;
                        decimal tienbshl = ((Money)a["new_sotienbosung"]).Value;
                        decimal tienbsphanbon = ((Money)a["new_bosungphanbon"]).Value;
                        decimal bosungtienmat = ((Money)a["new_bosungtienmat"]).Value;

                        dmkhl += tienbshl;
                        dmhl += tienbshl;

                        thuadatcanhtac["new_dinhmucdautukhonghoanlai"] = dmkhl;
                        thuadatcanhtac["new_dinhmucdautuhoanlai"] = dmhl;

                        service.Update(thuadatcanhtac);
                    }
                }
            }
        }

        public static EntityCollection FindCSDTBSbyKKPT(IOrganizationService crmservices, Entity KKPT, Entity vuDT, DateTime ngay)
        {
            string fetchXml =
                   @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                      <entity name='new_chinhsachdautuchitiet'>
                        <attribute name='new_chinhsachdautuchitietid' />
                        <attribute name='new_name' />
                        <attribute name='createdon' />
                        <attribute name='new_sotienbosung' />
                        <attribute name='new_sotienbosung_khl' />
                        <attribute name='new_bosungphanbon' />
                        <order attribute='new_name' descending='false' />
                        <filter type='and'>
                          <condition attribute='new_khuyenkhichphattrien' operator='eq' uitype='new_khuyenkhichphattrien' value='{0}' />
                          <condition attribute='new_nghiemthu' operator='eq' value='1'/>
                          <condition attribute='statecode' operator='eq' value='0' />
                          <condition attribute='new_vudautu' operator='eq' value='{1}' />
                          <condition attribute='new_tungay' operator='on-or-before' value='{2}' />
                          <condition attribute='new_denngay' operator='on-or-after' value='{3}' />
                        </filter>
                      </entity>
                    </fetch>";

            fetchXml = string.Format(fetchXml, KKPT.Id, vuDT.Id, ngay, ngay);
            EntityCollection entc = crmservices.RetrieveMultiple(new FetchExpression(fetchXml));

            return entc;
        }
    }
}
