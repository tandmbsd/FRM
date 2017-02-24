using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Activities;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;

namespace Plugin_KiemTraTyleGNkhiThemlanGNCSDT
{
    public class KiemTraTyleGNkhiThemlanGNCSDT : IPlugin
    {
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;

        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            service = factory.CreateOrganizationService(context.UserId);
            ITracingService traceService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
            {
                Entity Langiaingan = (Entity)context.InputParameters["Target"];
                Guid entityId = Langiaingan.Id;

                if (Langiaingan.LogicalName == "new_dinhmucdautu")
                {
                    //traceService.Trace("Begin plugin");
                    if (context.MessageName.ToUpper() == "CREATE")
                    {
                        Langiaingan = service.Retrieve("new_dinhmucdautu", entityId, new ColumnSet(true));
                        //DateTime ngaytao = Langiaingan.GetAttributeValue<DateTime>("createdon");   
                        if (Langiaingan.Attributes.Contains("new_chinhsachdautu"))
                        {
                            EntityReference csdtEntityRef = Langiaingan.GetAttributeValue<EntityReference>("new_chinhsachdautu");
                            Guid csdtId = csdtEntityRef.Id;
                            Entity csdtObj = service.Retrieve("new_chinhsachdautu", csdtId, new ColumnSet(new string[] { "new_name" }));

                            decimal phantramdaGN = 0;
                            decimal phantramconlai = 0;
                            decimal phantramlannay = (Langiaingan.Contains("new_phantramtilegiaingan") ? (decimal)Langiaingan["new_phantramtilegiaingan"] : 0);

                            string fetchXml =
                                      @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_dinhmucdautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_sotien' />
                                        <attribute name='new_langiaingan' />
                                        <attribute name='new_phantramtilegiaingan' />
                                        <attribute name='new_yeucauconghiemthu' />
                                        <attribute name='createdon' />
                                        <attribute name='new_dinhmucdautuid' />
                                        <order attribute='createdon' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                        </filter>
                                      </entity>
                                    </fetch>";
                            fetchXml = string.Format(fetchXml, csdtId);
                            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                            foreach (Entity a in result.Entities)
                            {
                                phantramdaGN += (a.Contains("new_phantramtilegiaingan") ? (decimal)a["new_phantramtilegiaingan"] : 0);
                            }
                            phantramconlai = 100 - phantramdaGN + phantramlannay;

                            if(phantramconlai <= 0)
                            {
                                throw new InvalidPluginExecutionException(" Tổng tỷ lệ phần trăm giải ngân đã đạt 100%");
                            }
                            if (phantramlannay > phantramconlai)
                            {
                                throw new InvalidPluginExecutionException(" Tỷ lệ phần trăm giải ngân tối đa còn lại là " + phantramconlai);
                            }
                        }
                    }

                    if (context.MessageName.ToUpper() == "UPDATE")
                    {
                        Langiaingan = service.Retrieve("new_dinhmucdautu", entityId, new ColumnSet(true));
                        if (Langiaingan.Attributes.Contains("new_chinhsachdautu"))
                        {
                            EntityReference csdtEntityRef = Langiaingan.GetAttributeValue<EntityReference>("new_chinhsachdautu");
                            Guid csdtId = csdtEntityRef.Id;
                            Entity csdtObj = service.Retrieve("new_chinhsachdautu", csdtId, new ColumnSet(new string[] { "new_name" }));

                            decimal phantramdaGN = 0;
                            decimal phantramconlai = 0;
                            decimal phantramlannay = (Langiaingan.Contains("new_phantramtilegiaingan") ? (decimal)Langiaingan["new_phantramtilegiaingan"] : 0);

                            string fetchXml =
                                      @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                                      <entity name='new_dinhmucdautu'>
                                        <attribute name='new_name' />
                                        <attribute name='new_sotien' />
                                        <attribute name='new_langiaingan' />
                                        <attribute name='new_phantramtilegiaingan' />
                                        <attribute name='new_yeucauconghiemthu' />
                                        <attribute name='createdon' />
                                        <attribute name='new_dinhmucdautuid' />
                                        <order attribute='createdon' descending='true' />
                                        <filter type='and'>
                                          <condition attribute='new_chinhsachdautu' operator='eq' uitype='new_chinhsachdautu' value='{0}' />
                                        </filter>
                                      </entity>
                                    </fetch>";
                            fetchXml = string.Format(fetchXml, csdtId);
                            EntityCollection result = service.RetrieveMultiple(new FetchExpression(fetchXml));
                            foreach (Entity a in result.Entities)
                            {
                                phantramdaGN += (a.Contains("new_phantramtilegiaingan") ? (decimal)a["new_phantramtilegiaingan"] : 0);
                            }
                            phantramconlai = 100 - phantramdaGN + phantramlannay;

                            if (phantramconlai <= 0)
                            {
                                throw new InvalidPluginExecutionException(" Tổng tỷ lệ phần trăm giải ngân đã đạt 100%");
                            }
                            if (phantramlannay > phantramconlai)
                            {
                                throw new InvalidPluginExecutionException(" Tỷ lệ phần trăm giải ngân tối đa còn lại là " + phantramconlai);
                            }
                        }
                    }
                }
            }  // if (context.InputParameters.Contains("Target") && (context.InputParameters["Target"] is Entity))
        }  // void IPlugin.Execute(IServiceProvider serviceProvider)
    }
}
