using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Messages;

namespace Plugin_PhieuDieuChinhCongNo
{
    public class Plugin_PhieuDieuChinhCongNo : IPlugin
    {
        //moi nhat
        IOrganizationService service = null;
        IOrganizationServiceFactory factory = null;
        ITracingService trace;
        void IPlugin.Execute(IServiceProvider serviceProvider)
        {
            trace = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

            if (context.MessageName.ToLower() == "update")
            {
                Entity target = (Entity)context.InputParameters["Target"];

                if (target.Contains("statuscode") && ((OptionSetValue)target["statuscode"]).Value == 100000001 && context.Depth < 2)
                {                    
                    factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
                    service = factory.CreateOrganizationService(context.UserId);

                    Entity phieudieuchinhcongno = service.Retrieve("new_phieudieuchinhcongno", target.Id, new ColumnSet(true));
                    string tinhtrang = ((OptionSetValue)phieudieuchinhcongno["statuscode"]).Value.ToString();

                    if (phieudieuchinhcongno.Contains("new_chitietbbgiamhuydientich"))
                    {
                        Entity ctbienbangiamhuy = service.Retrieve("new_chitietbbgiamhuydientich",
                            ((EntityReference)phieudieuchinhcongno["new_chitietbbgiamhuydientich"]).Id,
                            new ColumnSet(true));

                        Entity thuadatcanhtac = service.Retrieve("new_thuadatcanhtac",
                            ((EntityReference)phieudieuchinhcongno["new_chitiethddtmia"]).Id,
                            new ColumnSet(new string[] { "new_chitiethddtmiaid" }));

                        Entity hopdongdautumia = service.Retrieve("new_hopdongdautumia",
                            ((EntityReference)phieudieuchinhcongno["new_hopdongdautumia"]).Id,
                            new ColumnSet(new string[] { "new_vudautu" }));

                        QueryExpression q = new QueryExpression("new_phanbodautu");
                        q.ColumnSet = new ColumnSet(true);
                        q.Criteria.AddCondition(new ConditionExpression("new_hopdongdautumia", ConditionOperator.Equal,
                            ((EntityReference)phieudieuchinhcongno["new_hopdongdautumia"]).Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_vudautu", ConditionOperator.Equal,
                            ((EntityReference)hopdongdautumia["new_vudautu"]).Id));
                        q.Criteria.AddCondition(new ConditionExpression("new_thuacanhtac", ConditionOperator.Equal, thuadatcanhtac.Id));
                        EntityCollection entc = service.RetrieveMultiple(q);
                        
                        List<Entity> lstPBDT = entc.Entities.ToList<Entity>();

                        int count = lstPBDT.Count;                        
                        decimal sotiendieuchinh = ((Money)phieudieuchinhcongno["new_sotiendieuchinh"]).Value;
                        decimal sotienchiadeu = sotiendieuchinh / count;

                        foreach (Entity pbdt in lstPBDT)
                        {
                            if (sotiendieuchinh <= 0)                            
                                break;
                            
                            Entity newPBDT = service.Retrieve(pbdt.LogicalName, pbdt.Id,
                                new ColumnSet(new string[] { "new_sotien", "new_sotiendieuchinh" }));
                            decimal sotien = pbdt.Contains("new_sotien") ? ((Money)pbdt["new_sotien"]).Value : 0;

                            if (sotien >= sotiendieuchinh)
                            {
                                newPBDT["new_sotien"] = new Money(sotien - sotiendieuchinh);
                                newPBDT["new_sotiendieuchinh"] = new Money(sotiendieuchinh);
                                sotiendieuchinh = 0;
                            }
                            else
                            {
                                newPBDT["new_sotien"] = new Money(sotiendieuchinh - sotien);
                                newPBDT["new_sotiendieuchinh"] = new Money(sotien);
                                sotiendieuchinh = sotiendieuchinh - sotien;
                            }

                            service.Update(newPBDT);
                        }
                    }
                }
            }
        }
        int CompareDatetime(DateTime t1, DateTime t2)
        {
            int flag;
            if (t1.Year < t2.Year)
            {
                flag = -1;
            }
            else if (t1.Year == t2.Year)
            {
                if (t1.Month < t2.Month)
                {
                    flag = -1;
                }
                else if (t1.Month == t2.Month)
                {
                    if (t1.Day < t2.Day)
                    {
                        flag = -1;
                    }
                    else if (t1.Day == t2.Day)
                    {
                        flag = 0;
                    }
                    else
                        flag = 1;
                }
                else
                    flag = 1;
            }
            else
                flag = 1;


            return flag;
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
