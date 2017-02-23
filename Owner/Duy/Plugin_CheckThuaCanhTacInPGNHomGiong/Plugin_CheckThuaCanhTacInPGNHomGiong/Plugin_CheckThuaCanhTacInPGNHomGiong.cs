using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckThuaCanhTacInPGNHomGiong
{
    public class Plugin_CheckThuaCanhTacInPGNHomGiong : IPlugin
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

            Entity pgnhomgiong = new Entity();
            Entity thuadatcanhtac = new Entity();
            //String parameters = "";

            //foreach (KeyValuePair<string, object> attr in context.InputParameters)
            //{
            //    parameters += attr.Key.ToString();
            //}

            //throw new Exception(parameters);

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                //get the "relationship"
                if (context.InputParameters.Contains("Relationship"))
                {
                    relationshipName = context.InputParameters["Relationship"].ToString();
                }

                //check the relationshipname with intended one
                if (relationshipName != "new_new_pgnhomgiong_new_chitiethddtmia.")
                {
                    return;
                }

                // Get Entity 1 reference from “Target” Key from context

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    pgnhomgiong = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_hopdongdautumia" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    if (relatedEntities.Count > 0)
                    {                        
                        relatedEntity = relatedEntities[0];
                        thuadatcanhtac = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_name", "new_loaigocmia", "new_hopdongdautumia" }));
                        
                    }
                    else
                    {
                        return;
                    }
                }

                string loaigocmia = ((OptionSetValue)thuadatcanhtac["new_loaigocmia"]).Value.ToString();

                if (loaigocmia != "100000000")
                {
                    throw new Exception("Thửa đất phải là mía tơ mới được tạo vào PGN hom giống");
                }

                if (((EntityReference)pgnhomgiong["new_hopdongdautumia"]).Id != ((EntityReference)thuadatcanhtac["new_hopdongdautumia"]).Id)
                {
                    throw new Exception("Chi tiết hợp đồng đầu tư mía không cùng hợp đồng của PGN hom giống");
                }

                if (pgnhomgiong.Contains("new_khachhang") && thuadatcanhtac.Contains("new_khachhang") && ((EntityReference)pgnhomgiong["new_khachhang"]).Id != ((EntityReference)thuadatcanhtac["new_khachhang"]).Id)
                {
                    throw new Exception("Thửa đất canh tác " + thuadatcanhtac["new_name"].ToString() + " không cùng khách hàng với PGN hom giống");
                }

                if (pgnhomgiong.Contains("new_khachhangdoanhnghiep") && thuadatcanhtac.Contains("new_khachhangdoanhnghiep") && ((EntityReference)pgnhomgiong["new_khachhangdoanhnghiep"]).Id != ((EntityReference)thuadatcanhtac["new_khachhangdoanhnghiep"]).Id)
                {
                    throw new Exception("Thửa đất canh tác " + thuadatcanhtac["new_name"].ToString() + " không cùng khách hàng doanh nghiệp với PGN hom giống");
                }
            }
        }
    }
}
