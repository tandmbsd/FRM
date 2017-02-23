using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_UpdateThuaCanhTacFromPhanBon
{
    public class Plugin_UpdateThuaCanhTacFromPhanBon : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                Entity target = (Entity)context.InputParameters["Target"];
                if (context.MessageName.ToUpper() == "UPDATE" && target.Contains("statecode") && ((OptionSetValue)target["statecode"]).Value==1)
                {
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);
                    Entity bonphan = service.Retrieve(target.LogicalName, target.Id, new ColumnSet(true));
                    //throw new Exception(bonphan["subject"].ToString() );
                    Entity chitiethddtm = new Entity();

                    var Status = -1;
                    if (bonphan.Contains("statecode"))
                    {
                        Status = bonphan.GetAttributeValue<OptionSetValue>("statecode").Value;
                    }
                    if (Status == 1)
                    {
                        chitiethddtm = service.Retrieve("new_thuadatcanhtac", ((EntityReference)bonphan["new_thuacanhtac"]).Id, new ColumnSet(new string[] { "new_sat", "new_huuco", "new_landetieu", "new_nitodam", "new_kali", "new_canxi", "new_magie", "new_silic", "new_kem", "new_mangan", "new_molypden" }));

                        if (!chitiethddtm.Contains("new_sat"))
                        {
                            chitiethddtm["new_sat"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_huuco"))
                        {
                            chitiethddtm["new_huuco"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_landetieu"))
                        {
                            chitiethddtm["new_landetieu"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_nitodam"))
                        {
                            chitiethddtm["new_nitodam"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_kali"))
                        {
                            chitiethddtm["new_kali"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_canxi"))
                        {
                            chitiethddtm["new_canxi"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_magie"))
                        {
                            chitiethddtm["new_magie"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_silic"))
                        {
                            chitiethddtm["new_silic"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_kem"))
                        {
                            chitiethddtm["new_kem"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_mangan"))
                        {
                            chitiethddtm["new_mangan"] = new decimal(0);
                        }
                        if (!chitiethddtm.Contains("new_molypden"))
                        {
                            chitiethddtm["new_molypden"] = new decimal(0);
                        }
                        chitiethddtm["new_sat"] = chitiethddtm.GetAttributeValue<decimal>("new_sat") + bonphan.GetAttributeValue<decimal>("new_satfe");
                        chitiethddtm["new_huuco"] = chitiethddtm.GetAttributeValue<decimal>("new_huuco") + bonphan.GetAttributeValue<decimal>("new_huucoompvs");
                        chitiethddtm["new_landetieu"] = chitiethddtm.GetAttributeValue<decimal>("new_landetieu") + bonphan.GetAttributeValue<decimal>("new_landetieu");
                        chitiethddtm["new_nitodam"] = chitiethddtm.GetAttributeValue<decimal>("new_nitodam") + bonphan.GetAttributeValue<decimal>("new_nitodamn");
                        chitiethddtm["new_kali"] = chitiethddtm.GetAttributeValue<decimal>("new_kali") + bonphan.GetAttributeValue<decimal>("new_kali");
                        chitiethddtm["new_canxi"] = chitiethddtm.GetAttributeValue<decimal>("new_canxi") + bonphan.GetAttributeValue<decimal>("new_canxi");
                        chitiethddtm["new_magie"] = chitiethddtm.GetAttributeValue<decimal>("new_magie") + bonphan.GetAttributeValue<decimal>("new_magie");
                        chitiethddtm["new_silic"] = chitiethddtm.GetAttributeValue<decimal>("new_silic") + bonphan.GetAttributeValue<decimal>("new_silic");
                        chitiethddtm["new_kem"] = chitiethddtm.GetAttributeValue<decimal>("new_kem") + bonphan.GetAttributeValue<decimal>("new_kemzn");
                        chitiethddtm["new_mangan"] = chitiethddtm.GetAttributeValue<decimal>("new_mangan") + bonphan.GetAttributeValue<decimal>("new_mangan");
                        chitiethddtm["new_molypden"] = chitiethddtm.GetAttributeValue<decimal>("new_molypden") + bonphan.GetAttributeValue<decimal>("new_molypdenmo");
                        //throw new Exception(chitiethddtm.GetAttributeValue<decimal>("new_luongnuoctuoihuuhieu").ToString());
                        service.Update(chitiethddtm);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidPluginExecutionException(ex.Message);
            }
        }
    }
}
