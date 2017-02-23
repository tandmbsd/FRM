using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckCTHDDTMiainPDKHomGiong
{
    public class Plugin_CheckCTHDDTMiainPDKHomGiong : IPlugin
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

            Entity pdkhomgiong = new Entity();
            Entity thuadatcanhtac = new Entity();

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                if (context.InputParameters.Contains("Relationship"))
                {
                    relationshipName = context.InputParameters["Relationship"].ToString();
                }

                if (relationshipName != "new_new_pdkhomgiong_new_chitiethddtmia.")
                {
                    return;
                }

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    pdkhomgiong = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_hopdongdautumia" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    if (relatedEntities.Count > 0)
                    {
                        relatedEntity = relatedEntities[0];
                        thuadatcanhtac = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] { "new_hopdongdautumia", "new_loaigocmia" }));
                    }
                    else
                    {
                        return;
                    }
                }

                if (((EntityReference)pdkhomgiong["new_hopdongdautumia"]).Id != ((EntityReference)thuadatcanhtac["new_hopdongdautumia"]).Id)
                {
                    throw new Exception("Chi tiết hợp đồng đầu tư mía không thuộc hợp đồng đầu tư mía đã chọn !!!");
                }

                string loaigocmia = ((OptionSetValue)thuadatcanhtac["new_loaigocmia"]).Value.ToString();

                if (loaigocmia != "100000000")
                {
                    throw new Exception("Thửa đất phải là mía tơ mới được tạo vào PDK hom giống");
                }
            }
        }
    }
}
