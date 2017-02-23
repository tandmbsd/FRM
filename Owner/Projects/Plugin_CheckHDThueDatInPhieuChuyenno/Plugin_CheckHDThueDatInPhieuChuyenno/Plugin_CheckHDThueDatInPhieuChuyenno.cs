using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_CheckHDThueDatInPhieuChuyenno
{
    public class Plugin_CheckHDThueDatInPhieuChuyenno : IPlugin
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

            Entity phieuchuyenno = new Entity();
            Entity hopdongthuedat = new Entity();

            if (context.MessageName.ToLower().Trim() == "associate")
            {
                if (context.InputParameters.Contains("Relationship"))
                {
                    relationshipName = context.InputParameters["Relationship"].ToString();
                }

                if (relationshipName != "new_new_phieuchuyenno_new_hopdongthuedat.")
                {
                    return;
                }

                if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                {
                    targetEntity = (EntityReference)context.InputParameters["Target"];
                    phieuchuyenno = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_khachhang_bena", "new_khachhangdoanhnghiep_bena" }));
                }

                if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                {
                    relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                    if (relatedEntities.Count > 0)
                    {
                        relatedEntity = relatedEntities[0];
                        hopdongthuedat = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] { "new_khachhang", "new_khachhangdoanhnghiep", "new_name" }));
                    }
                    else
                    {
                        return;
                    }
                }

                if ((!phieuchuyenno.Contains("new_khachhang_bena")) && (!hopdongthuedat.Contains("new_khachhang")) && (!phieuchuyenno.Contains("new_khachhangdoanhnghiep_bena")) && (!hopdongthuedat.Contains("new_khachhangdoanhnghiep")))
                {
                    throw new Exception("Hợp đồng thuê đất" + hopdongthuedat["new_name"].ToString() + " không hợp lệ");
                }

                if (phieuchuyenno.Contains("new_khachhang_bena"))
                {
                    if (hopdongthuedat.Contains("new_khachhangdoanhnghiep"))
                    {
                        throw new Exception("Hợp đồng thuê đất " + hopdongthuedat["new_name"].ToString() + " không hợp lệ");
                    }
                    else if (hopdongthuedat.Contains("new_khachhang"))
                    {
                        if (((EntityReference)phieuchuyenno["new_khachhang_bena"]).Id != ((EntityReference)hopdongthuedat["new_khachhang"]).Id)
                        {
                            throw new Exception("Hợp đồng thuê đất" + hopdongthuedat["new_name"].ToString() + " không hợp lệ");
                        }
                    }
                }

                else if (phieuchuyenno.Contains("new_khachhangdoanhnghiep_bena"))
                {
                    if (hopdongthuedat.Contains("new_khachhang"))
                    {
                        throw new Exception("Hợp đồng thuê đất " + hopdongthuedat["new_name"].ToString() + " không hợp lệ");
                    }
                    else if (hopdongthuedat.Contains("new_khachhangdoanhnghiep"))
                    {
                        if (((EntityReference)phieuchuyenno["new_khachhangdoanhnghiep_bena"]).Id != ((EntityReference)hopdongthuedat["new_khachhangdoanhnghiep"]).Id)
                        {
                            throw new Exception("Hợp đồng thuê đất " + hopdongthuedat["new_name"].ToString() + " không hợp lệ");
                        }
                    }
                }
            }
        }
    }
}
