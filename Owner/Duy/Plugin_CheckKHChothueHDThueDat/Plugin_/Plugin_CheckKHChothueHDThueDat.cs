using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;


namespace Plugin_CheckKHChothueHDThueDat
{
    public class Plugin_CheckKHChothueHDThueDat : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            try
            {
                IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
                factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));

                service = factory.CreateOrganizationService(context.UserId);

                EntityReference targetEntity = null;
                string relationshipName = string.Empty;
                EntityReferenceCollection relatedEntities = null;
                EntityReference relatedEntity = null;

                Entity datthue = new Entity();
                Entity thuadat = new Entity();
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
                    if (relationshipName != "new_new_datthue_new_thuadat.")
                    {
                        return;
                    }

                    // Get Entity 1 reference from “Target” Key from context

                    if (context.InputParameters.Contains("Target") && context.InputParameters["Target"] is EntityReference)
                    {
                        targetEntity = (EntityReference)context.InputParameters["Target"];
                        datthue = service.Retrieve(targetEntity.LogicalName, targetEntity.Id, new ColumnSet(new string[] { "new_benchothuedatkh", "new_benchothuedatkhdn", "new_name" }));
                    }

                    if (context.InputParameters.Contains("RelatedEntities") && context.InputParameters["RelatedEntities"] is EntityReferenceCollection)
                    {
                        relatedEntities = context.InputParameters["RelatedEntities"] as EntityReferenceCollection;
                        if (relatedEntities.Count > 0)
                        {
                            relatedEntity = relatedEntities[0];
                            thuadat = service.Retrieve(relatedEntity.LogicalName, relatedEntity.Id, new ColumnSet(new string[] { "new_chusohuuchinhtd", "new_chusohuuchinhtdkhdn", "new_name" }));
                        }
                        else
                        {
                            return;
                        }
                    }
                    //throw new Exception(((EntityReference)datthue["new_benchothuedatkh"]).Name+ ((EntityReference)thuadat["new_chusohuuchinhtd"]).Name);
                    if ((!datthue.Contains("new_benchothuedatkh")) && (!thuadat.Contains("new_chusohuuchinhtd")) && (!datthue.Contains("new_benchothuedatkhdn")) && (!thuadat.Contains("new_chusohuuchinhtdkhdn")))
                    {
                        throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                    }

                    if (datthue.Contains("new_benchothuedatkh"))
                    {
                        if (thuadat.Contains("new_chusohuuchinhtdkhdn"))
                        {
                            throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                        }
                        else if (thuadat.Contains("new_chusohuuchinhtd"))
                        {
                            if (((EntityReference)datthue["new_benchothuedatkh"]).Id != ((EntityReference)thuadat["new_chusohuuchinhtd"]).Id)
                            {
                                throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                            }
                        }
                    }

                    else if (datthue.Contains("new_benchothuedatkhdn"))
                    {
                        if (thuadat.Contains("new_chusohuuchinhtd"))
                        {
                            throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                        }
                        else if (thuadat.Contains("new_chusohuuchinhtdkhdn"))
                        {
                            if (((EntityReference)datthue["new_benchothuedatkhdn"]).Id != ((EntityReference)thuadat["new_chusohuuchinhtdkhdn"]).Id)
                            {
                                throw new Exception("Thửa đất " + thuadat["new_name"].ToString() + " không thuộc người cho thuê");
                            }
                        }
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
